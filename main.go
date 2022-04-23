// Copyright (C) 2022 Charalampos Mitsakis (go.mitsakis.org/workerpool)
// Licensed under the Apache License, Version 2.0

// Generic worker pool with limited concurrency, backpressure, and dynamically resizable number of workers.
package workerpool

import (
	"fmt"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type Job[P any] struct {
	Payload P
	ID      int
	Attempt int
}

type Result[P, R any] struct {
	Job   Job[P]
	Value R
	Error error
}

type Pool[P any, R any, C any] struct {
	maxActiveWorkers int
	retries          int
	reinitDelay      time.Duration
	idleTimeout      time.Duration
	targetLoad       float64
	name             string
	loggerInfo       *log.Logger
	loggerDebug      *log.Logger
	handler          func(job Job[P], workerID int, connection C) (R, error)
	workerInit       func(workerID int) (C, error)
	workerDeinit     func(workerID int, connection C) error
	concurrency      int32
	jobsNew          chan P
	jobsQueue        chan Job[P]
	wgJobs           sync.WaitGroup
	wgWorkers        sync.WaitGroup
	nJobsProcessing  int32
	jobsDone         chan struct{}
	Results          chan Result[P, R]
	enableWorker     chan struct{}
	disableWorker    chan struct{}
}

type poolConfig struct {
	retries     int
	reinitDelay time.Duration
	idleTimeout time.Duration
	targetLoad  float64
	name        string
	loggerInfo  *log.Logger
	loggerDebug *log.Logger
}

// Retries sets the number of times a job will be retried if it fails.
func Retries(n int) func(c *poolConfig) error {
	return func(c *poolConfig) error {
		c.retries = n
		return nil
	}
}

// ReinitDelay sets the time duration the worker should wait before reattempting init after failure.
func ReinitDelay(d time.Duration) func(c *poolConfig) error {
	return func(c *poolConfig) error {
		c.reinitDelay = d
		return nil
	}
}

// IdleTimeout sets the time duration after which an idle worker is stopped.
func IdleTimeout(d time.Duration) func(c *poolConfig) error {
	return func(c *poolConfig) error {
		c.idleTimeout = d
		return nil
	}
}

// TargetLoad sets the target load of the pool.
//
// load = n / c,
// where n = number of jobs in queue or processing,
// and c = concurrency (current number of started workers).
//
// If load is higher than target load, new workers are started.
// If it's lower, some workers are stopped.
func TargetLoad(v float64) func(c *poolConfig) error {
	return func(c *poolConfig) error {
		if v > 1 {
			return fmt.Errorf("TargetLoad() invalid argument (v > 1)")
		} else if v <= 0 {
			return fmt.Errorf("TargetLoad() invalid argument (v <= 0)")
		}
		c.targetLoad = v
		return nil
	}
}

// Name sets the name of the pool.
func Name(s string) func(c *poolConfig) error {
	return func(c *poolConfig) error {
		c.name = s
		return nil
	}
}

// LoggerInfo sets a logger for info level logging.
func LoggerInfo(l *log.Logger) func(c *poolConfig) error {
	return func(c *poolConfig) error {
		c.loggerInfo = l
		return nil
	}
}

// LoggerDebug sets a logger for debug level logging.
func LoggerDebug(l *log.Logger) func(c *poolConfig) error {
	return func(c *poolConfig) error {
		c.loggerDebug = l
		return nil
	}
}

// NewPoolSimple creates a new worker pool.
func NewPoolSimple[P any](maxActiveWorkers int, handler func(job Job[P], workerID int) error, options ...func(*poolConfig) error) (*Pool[P, struct{}, struct{}], error) {
	handler2 := func(job Job[P], workerID int, connection struct{}) error {
		return handler(job, workerID)
	}
	return NewPoolWithInit[P, struct{}](maxActiveWorkers, handler2, nil, nil, options...)
}

// NewPoolWithInit creates a new worker pool with workerInit() and workerDeinit() functions.
func NewPoolWithInit[P, C any](maxActiveWorkers int, handler func(job Job[P], workerID int, connection C) error, workerInit func(workerID int) (C, error), workerDeinit func(workerID int, connection C) error, options ...func(*poolConfig) error) (*Pool[P, struct{}, C], error) {
	handler2 := func(job Job[P], workerID int, connection C) (struct{}, error) {
		return struct{}{}, handler(job, workerID, connection)
	}
	return newPool[P, struct{}, C](maxActiveWorkers, handler2, nil, nil, false, options...)
}

// NewPoolWithResults creates a new worker pool with Results channel. You should consume from this channel until it is closed.
func NewPoolWithResults[P, R any](maxActiveWorkers int, handler func(job Job[P], workerID int) (R, error), options ...func(*poolConfig) error) (*Pool[P, R, struct{}], error) {
	handler2 := func(job Job[P], workerID int, connection struct{}) (R, error) {
		return handler(job, workerID)
	}
	return newPool[P, R, struct{}](maxActiveWorkers, handler2, nil, nil, true, options...)
}

// NewPoolWithInitResults creates a new worker pool with workerInit() and workerDeinit() functions and Results channel. You should consume from this channel until it is closed.
func NewPoolWithInitResults[P, R, C any](maxActiveWorkers int, handler func(job Job[P], workerID int, connection C) (R, error), workerInit func(workerID int) (C, error), workerDeinit func(workerID int, connection C) error, options ...func(*poolConfig) error) (*Pool[P, R, C], error) {
	return newPool[P, R, C](maxActiveWorkers, handler, workerInit, workerDeinit, true, options...)
}

func newPool[P, R, C any](maxActiveWorkers int, handler func(job Job[P], workerID int, connection C) (R, error), workerInit func(workerID int) (C, error), workerDeinit func(workerID int, connection C) error, createResultsChannel bool, options ...func(*poolConfig) error) (*Pool[P, R, C], error) {
	// default configuration
	config := poolConfig{
		reinitDelay: time.Second,
		idleTimeout: 20 * time.Second,
		targetLoad:  0.9,
	}
	for _, option := range options {
		err := option(&config)
		if err != nil {
			return nil, fmt.Errorf("config error: %s", err)
		}
	}

	var loggerInfo *log.Logger
	if config.loggerInfo != nil {
		if config.name == "" {
			loggerInfo = config.loggerInfo
		} else {
			loggerInfo = log.New(config.loggerInfo.Writer(), config.loggerInfo.Prefix()+"[pool="+config.name+"] ", config.loggerInfo.Flags()|log.Lmsgprefix)
		}
	}
	var loggerDebug *log.Logger
	if config.loggerDebug != nil {
		if config.name == "" {
			loggerDebug = config.loggerDebug
		} else {
			loggerDebug = log.New(config.loggerDebug.Writer(), config.loggerDebug.Prefix()+"[pool="+config.name+"] ", config.loggerDebug.Flags()|log.Lmsgprefix)
		}
	}

	p := Pool[P, R, C]{
		retries:          config.retries,
		reinitDelay:      config.reinitDelay,
		idleTimeout:      config.idleTimeout,
		targetLoad:       config.targetLoad,
		name:             config.name,
		loggerInfo:       loggerInfo,
		loggerDebug:      loggerDebug,
		maxActiveWorkers: maxActiveWorkers,
		handler:          handler,
		workerInit:       workerInit,
		workerDeinit:     workerDeinit,
	}
	if p.maxActiveWorkers == 0 {
		return nil, fmt.Errorf("maxActiveWorkers = 0")
	}
	p.jobsNew = make(chan P, 2)
	p.jobsQueue = make(chan Job[P], p.maxActiveWorkers) // size p.maxActiveWorkers in order to avoid deadlock
	p.jobsDone = make(chan struct{}, p.maxActiveWorkers)
	if createResultsChannel {
		p.Results = make(chan Result[P, R])
	}
	p.enableWorker = make(chan struct{}, 1)
	p.disableWorker = make(chan struct{})
	go p.loop()
	for i := 0; i < p.maxActiveWorkers; i++ {
		w := newWorker(&p, i)
		p.wgWorkers.Add(1)
		go w.loop()
	}
	return &p, nil
}

func (p *Pool[P, R, C]) loop() {
	var loadAvg float64 = 1
	var jobID int
	var jobIDWhenLastEnabledWorker int
	var doneCounter int
	var doneCounterWhenLastDisabledWorker int
	var nJobsInSystem int
	var jobDone bool
	for p.jobsNew != nil || p.jobsDone != nil {
		concurrency := atomic.LoadInt32(&p.concurrency)
		if concurrency > 0 {
			// concurrency > 0 so we can divide
			loadNow := float64(nJobsInSystem) / float64(concurrency)
			const a = 0.1
			loadAvg = a*loadNow + (1-a)*loadAvg
			if p.loggerDebug != nil {
				nJobsProcessing := atomic.LoadInt32(&p.nJobsProcessing)
				if jobDone {
					p.loggerDebug.Printf("[workerpool/loop] len(jobsNew)=%d len(jobsQueue)=%d nJobsProcessing=%d nJobsInSystem=%d concurrency=%d loadAvg=%.2f doneCounter=%d\n", len(p.jobsNew), len(p.jobsQueue), nJobsProcessing, nJobsInSystem, concurrency, loadAvg, doneCounter)
				} else {
					p.loggerDebug.Printf("[workerpool/loop] len(jobsNew)=%d len(jobsQueue)=%d nJobsProcessing=%d nJobsInSystem=%d concurrency=%d loadAvg=%.2f jobID=%d\n", len(p.jobsNew), len(p.jobsQueue), nJobsProcessing, nJobsInSystem, concurrency, loadAvg, jobID)
				}
			}
		}
		if !jobDone && loadAvg > p.targetLoad*float64(concurrency+1)/float64(concurrency) && jobID-jobIDWhenLastEnabledWorker > 20 {
			// if load is high and we haven't enabled a worker recently, enable n workers
			// n = number of workers we should enable
			// find n such that:
			// loadAvg < p.targetLoad*(concurrency+n)/concurrency
			// loadAvg*concurrency/p.targetLoad < concurrency+n
			// loadAvg*concurrency/p.targetLoad - concurrency < n
			// calculate square root to keep n low if loadAvg is high (otherwise we might enable too many workers)
			n := int(math.Sqrt(loadAvg*float64(concurrency)/p.targetLoad - float64(concurrency)))
			if n > 0 {
				if p.loggerDebug != nil {
					p.loggerDebug.Printf("[workerpool/loop] [jobID=%d] high load - enabling %d new workers", jobID, n)
				}
				// try to enable n workers.
				for i := 0; i < n; i++ {
					select {
					case p.enableWorker <- struct{}{}:
					default:
					}
				}
				jobIDWhenLastEnabledWorker = jobID
			}
		}
		if concurrency > 0 && jobDone {
			if loadAvg < p.targetLoad*float64(concurrency-1)/float64(concurrency) && doneCounter-doneCounterWhenLastDisabledWorker > 20 {
				// if load is low and we didn't disable a worker recently, disable n workers
				// n = number of workers we should disable
				// find n such that:
				// loadAvg > p.targetLoad*(concurrency-n)/concurrency
				// loadAvg*concurrency/p.targetLoad > concurrency-n
				// n + loadAvg*concurrency/p.targetLoad > concurrency
				// n > concurrency - loadAvg*concurrency/p.targetLoad
				n := int(float64(concurrency) - loadAvg*float64(concurrency)/p.targetLoad)
				if int(concurrency)-n <= 0 {
					n = int(concurrency) - 1
				}
				if n > 0 {
					if p.loggerDebug != nil {
						p.loggerDebug.Printf("[workerpool/loop] [doneCounter=%d] low load - disabling %v workers", doneCounter, n)
					}
					// try to disable n workers.
					for i := 0; i < n; i++ {
						select {
						case p.disableWorker <- struct{}{}:
						default:
							// no worker is listening to the disabledWorker channel so write will be lost but this is not a problem
							// it means all workers are busy so maybe we shouldn't disable any worker
						}
					}
					doneCounterWhenLastDisabledWorker = doneCounter
				}
			}
		} else if concurrency == 0 {
			loadAvg = 1
		}
		jobDone = false
		// make sure not all workers are disabled while there are jobs
		if concurrency == 0 && nJobsInSystem > 0 {
			if p.loggerDebug != nil {
				p.loggerDebug.Printf("[workerpool/loop] [doneCounter=%d] no active worker. try to enable new worker", doneCounter)
			}
		drainLoop:
			for {
				select {
				case <-p.disableWorker:
				default:
					break drainLoop
				}
			}
			select {
			case p.enableWorker <- struct{}{}:
			default:
			}
		}
		if nJobsInSystem >= p.maxActiveWorkers {
			// if there are p.maxActiveWorkers jobs don't accept new jobs
			// len(p.jobsQueue) = p.maxActiveWorkers
			// that way we make sure nJobsInSystem < len(p.jobsQueue)
			// so writes to p.jobsQueue don't block
			// blocking writes to p.jobsQueue would cause deadlock
			_, ok := <-p.jobsDone
			if !ok {
				p.jobsDone = nil
				continue
			}
			nJobsInSystem--
			doneCounter++
			jobDone = true
		} else {
			select {
			case payload, ok := <-p.jobsNew:
				if !ok {
					p.jobsNew = nil
					continue
				}
				nJobsInSystem++
				p.jobsQueue <- Job[P]{Payload: payload, ID: jobID, Attempt: 0}
				jobID++
			case _, ok := <-p.jobsDone:
				if !ok {
					p.jobsDone = nil
					continue
				}
				nJobsInSystem--
				doneCounter++
				jobDone = true
			}
		}
	}
	close(p.jobsQueue)
	close(p.enableWorker)
	close(p.disableWorker)
	if p.loggerDebug != nil {
		p.loggerDebug.Println("[workerpool/loop] finished")
	}
}

// Submit adds a new job to the queue.
func (p *Pool[P, R, C]) Submit(jobPayload P) {
	p.wgJobs.Add(1)
	select {
	case p.jobsNew <- jobPayload:
	default:
		if p.loggerDebug != nil {
			p.loggerDebug.Println("[workerpool/Submit] blocked. try to enable new worker")
		}
		select {
		case p.enableWorker <- struct{}{}:
		default:
		}
		p.jobsNew <- jobPayload
	}
}

// StopAndWait shuts down the pool.
// Once called no more jobs can be submitted,
// and waits for all enqueued jobs to finish and workers to stop.
func (p *Pool[P, R, C]) StopAndWait() {
	close(p.jobsNew)
	if p.loggerDebug != nil {
		p.loggerDebug.Println("[workerpool/StopAndWait] waiting for all jobs to finish")
	}
	p.wgJobs.Wait()
	close(p.jobsDone)
	if p.loggerDebug != nil {
		p.loggerDebug.Println("[workerpool/StopAndWait] waiting for all workers to finish")
	}
	p.wgWorkers.Wait()
	if p.loggerDebug != nil {
		p.loggerDebug.Println("[workerpool/StopAndWait] finished")
	}
	if p.Results != nil {
		close(p.Results)
	}
}

type worker[P, R, C any] struct {
	id         int
	pool       *Pool[P, R, C]
	connection *C
	idleTicker *time.Ticker
}

func newWorker[P, R, C any](p *Pool[P, R, C], id int) *worker[P, R, C] {
	return &worker[P, R, C]{
		id:   id,
		pool: p,
	}
}

func (w *worker[P, R, C]) loop() {
	enabled := false
	deinit := func() {
		if w.idleTicker != nil {
			w.idleTicker.Stop()
		}
		if enabled {
			enabled = false
			atomic.AddInt32(&w.pool.concurrency, -1)
			if w.pool.workerDeinit != nil {
				err := w.pool.workerDeinit(w.id, *w.connection)
				if err != nil {
					w.pool.loggerInfo.Printf("[workerpool/worker%d] workerDeinit failed: %s\n", w.id, err)
				}
				w.connection = nil
			}
			if w.pool.loggerDebug != nil {
				concurrency := atomic.LoadInt32(&w.pool.concurrency)
				w.pool.loggerDebug.Printf("[workerpool/worker%d] worker disabled - concurrency %d\n", w.id, concurrency)
			}
		}
	}
	defer w.pool.wgWorkers.Done()
	defer deinit()
loop:
	for {
		if !enabled {
			if w.idleTicker != nil {
				w.idleTicker.Stop()
			}
			_, ok := <-w.pool.enableWorker
			if !ok {
				break
			}
			enabled = true
			atomic.AddInt32(&w.pool.concurrency, 1)
			if w.pool.loggerDebug != nil {
				w.pool.loggerDebug.Printf("[workerpool/worker%d] worker enabled\n", w.id)
			}
			if w.pool.workerInit != nil {
				connection, err := w.pool.workerInit(w.id)
				if err != nil {
					w.pool.loggerInfo.Printf("[workerpool/worker%d] workerInit failed: %s\n", w.id, err)
					time.Sleep(w.pool.reinitDelay)
					continue
				}
				w.connection = &connection
			} else {
				w.connection = new(C)
			}
			w.idleTicker = time.NewTicker(w.pool.idleTimeout)
		}
		select {
		case <-w.idleTicker.C:
			deinit()
		case _, ok := <-w.pool.disableWorker:
			if !ok {
				break loop
			}
			deinit()
		case j, ok := <-w.pool.jobsQueue:
			if !ok {
				break loop
			}
			atomic.AddInt32(&w.pool.nJobsProcessing, 1)
			resultValue, err := w.pool.handler(j, w.id, *w.connection)
			atomic.AddInt32(&w.pool.nJobsProcessing, -1)
			w.idleTicker.Stop()
			w.idleTicker = time.NewTicker(w.pool.idleTimeout)
			if err != nil && errorIsRetryable(err) && j.Attempt < w.pool.retries {
				j.Attempt++
				w.pool.jobsQueue <- j
			} else {
				if w.pool.Results != nil {
					w.pool.Results <- Result[P, R]{Job: j, Value: resultValue, Error: err}
				}
				w.pool.jobsDone <- struct{}{}
				w.pool.wgJobs.Done()
			}
		}
	}
	if w.pool.loggerDebug != nil {
		w.pool.loggerDebug.Printf("[workerpool/worker%d] finished\n", w.id)
	}
}
