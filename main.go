// Copyright (C) 2022 Charalampos Mitsakis (go.mitsakis.org/workerpool)
// Licensed under the Apache License, Version 2.0

// Generic worker pool (work queue) library with auto-scaling, backpressure, and easy composability of pools into pipelines.
package workerpool

import (
	"context"
	"fmt"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type Job[I any] struct {
	Payload I
	ID      int
	Attempt int
}

type Result[I, O any] struct {
	Job   Job[I]
	Value O
	Error error
}

type Pool[I, O, C any] struct {
	maxActiveWorkers int
	fixedWorkers     bool
	retries          int
	reinitDelay      time.Duration
	idleTimeout      time.Duration
	targetLoad       float64
	name             string
	loggerInfo       *log.Logger
	loggerDebug      *log.Logger
	handler          func(job Job[I], workerID int, connection C) (O, error)
	workerInit       func(workerID int) (C, error)
	workerDeinit     func(workerID int, connection C) error
	concurrency      int32
	concurrencyIs0   chan struct{}
	jobsNew          chan I
	jobsQueue        chan Job[I]
	wgJobs           sync.WaitGroup
	wgWorkers        sync.WaitGroup
	nJobsProcessing  int32
	jobsDone         chan Result[I, O]
	Results          chan Result[I, O]
	enableWorker     chan struct{}
	disableWorker    chan struct{}
	monitor          func(s stats)
	cancelWorkers    context.CancelFunc
	loopDone         chan struct{}
}

type poolConfig struct {
	setOptions   map[int]struct{}
	fixedWorkers bool
	retries      int
	reinitDelay  time.Duration
	idleTimeout  time.Duration
	targetLoad   float64
	name         string
	loggerInfo   *log.Logger
	loggerDebug  *log.Logger
	monitor      func(s stats)
}

const (
	optionFixedWorkers = iota
	optionRetries
	optionReinitDelay
	optionIdleTimeout
	optionTargetLoad
	optionName
	optionLoggerInfo
	optionLoggerDebug
)

// FixedWorkers disables auto-scaling and makes the pool use a fixed number of workers equal to the value of maxActiveWorkers.
func FixedWorkers() func(c *poolConfig) error {
	return func(c *poolConfig) error {
		c.fixedWorkers = true
		c.setOptions[optionFixedWorkers] = struct{}{}
		return nil
	}
}

// Retries sets the number of times a job will be retried if it fails with a retryable error (see function ErrorWrapRetryable).
//
// Default value = 1
func Retries(n int) func(c *poolConfig) error {
	return func(c *poolConfig) error {
		c.retries = n
		c.setOptions[optionRetries] = struct{}{}
		return nil
	}
}

// ReinitDelay sets the time duration the worker should wait before reattempting init after failure.
func ReinitDelay(d time.Duration) func(c *poolConfig) error {
	return func(c *poolConfig) error {
		c.reinitDelay = d
		c.setOptions[optionReinitDelay] = struct{}{}
		return nil
	}
}

// IdleTimeout sets the time duration after which an idle worker is stopped.
func IdleTimeout(d time.Duration) func(c *poolConfig) error {
	return func(c *poolConfig) error {
		c.idleTimeout = d
		c.setOptions[optionIdleTimeout] = struct{}{}
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
		c.setOptions[optionTargetLoad] = struct{}{}
		return nil
	}
}

// Name sets the name of the pool.
func Name(s string) func(c *poolConfig) error {
	return func(c *poolConfig) error {
		c.name = s
		c.setOptions[optionName] = struct{}{}
		return nil
	}
}

// LoggerInfo sets a logger for info level logging.
func LoggerInfo(l *log.Logger) func(c *poolConfig) error {
	return func(c *poolConfig) error {
		c.loggerInfo = l
		c.setOptions[optionLoggerInfo] = struct{}{}
		return nil
	}
}

// LoggerDebug sets a logger for debug level logging.
func LoggerDebug(l *log.Logger) func(c *poolConfig) error {
	return func(c *poolConfig) error {
		c.loggerDebug = l
		c.setOptions[optionLoggerDebug] = struct{}{}
		return nil
	}
}

func monitor(f func(s stats)) func(c *poolConfig) error {
	return func(c *poolConfig) error {
		c.monitor = f
		return nil
	}
}

// NewPoolSimple creates a new worker pool.
func NewPoolSimple[I any](maxActiveWorkers int, handler func(job Job[I], workerID int) error, options ...func(*poolConfig) error) (*Pool[I, struct{}, struct{}], error) {
	handler2 := func(job Job[I], workerID int, connection struct{}) error {
		return handler(job, workerID)
	}
	return NewPoolWithInit[I, struct{}](maxActiveWorkers, handler2, nil, nil, options...)
}

// NewPoolWithInit creates a new worker pool with workerInit() and workerDeinit() functions.
func NewPoolWithInit[I, C any](maxActiveWorkers int, handler func(job Job[I], workerID int, connection C) error, workerInit func(workerID int) (C, error), workerDeinit func(workerID int, connection C) error, options ...func(*poolConfig) error) (*Pool[I, struct{}, C], error) {
	handler2 := func(job Job[I], workerID int, connection C) (struct{}, error) {
		return struct{}{}, handler(job, workerID, connection)
	}
	return newPool[I, struct{}, C](maxActiveWorkers, handler2, workerInit, workerDeinit, false, options...)
}

// NewPoolWithResults creates a new worker pool with Results channel. You should consume from this channel until it is closed.
func NewPoolWithResults[I, O any](maxActiveWorkers int, handler func(job Job[I], workerID int) (O, error), options ...func(*poolConfig) error) (*Pool[I, O, struct{}], error) {
	handler2 := func(job Job[I], workerID int, connection struct{}) (O, error) {
		return handler(job, workerID)
	}
	return newPool[I, O, struct{}](maxActiveWorkers, handler2, nil, nil, true, options...)
}

// NewPoolWithResultsAndInit creates a new worker pool with workerInit() and workerDeinit() functions and Results channel. You should consume from this channel until it is closed.
func NewPoolWithResultsAndInit[I, O, C any](maxActiveWorkers int, handler func(job Job[I], workerID int, connection C) (O, error), workerInit func(workerID int) (C, error), workerDeinit func(workerID int, connection C) error, options ...func(*poolConfig) error) (*Pool[I, O, C], error) {
	return newPool[I, O, C](maxActiveWorkers, handler, workerInit, workerDeinit, true, options...)
}

func newPool[I, O, C any](maxActiveWorkers int, handler func(job Job[I], workerID int, connection C) (O, error), workerInit func(workerID int) (C, error), workerDeinit func(workerID int, connection C) error, createResultsChannel bool, options ...func(*poolConfig) error) (*Pool[I, O, C], error) {
	// default configuration
	config := poolConfig{
		setOptions:  make(map[int]struct{}),
		retries:     1,
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
	_, setFixedWorkers := config.setOptions[optionFixedWorkers]
	_, setTargetLoad := config.setOptions[optionTargetLoad]
	if setFixedWorkers && setTargetLoad {
		return nil, fmt.Errorf("options FixedWorkers() and TargetLoad() are incompatible")
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

	ctxWorkers, cancelWorkers := context.WithCancel(context.Background())

	p := Pool[I, O, C]{
		retries:          config.retries,
		reinitDelay:      config.reinitDelay,
		idleTimeout:      config.idleTimeout,
		targetLoad:       config.targetLoad,
		name:             config.name,
		loggerInfo:       loggerInfo,
		loggerDebug:      loggerDebug,
		monitor:          config.monitor,
		maxActiveWorkers: maxActiveWorkers,
		fixedWorkers:     config.fixedWorkers,
		handler:          handler,
		workerInit:       workerInit,
		workerDeinit:     workerDeinit,
		cancelWorkers:    cancelWorkers,
	}
	if p.maxActiveWorkers == 0 {
		return nil, fmt.Errorf("maxActiveWorkers = 0")
	}
	p.jobsNew = make(chan I, 2)
	p.jobsQueue = make(chan Job[I], p.maxActiveWorkers) // size p.maxActiveWorkers in order to avoid deadlock
	p.jobsDone = make(chan Result[I, O], p.maxActiveWorkers)
	if createResultsChannel {
		p.Results = make(chan Result[I, O], p.maxActiveWorkers)
	}
	p.concurrencyIs0 = make(chan struct{}, 1)
	p.enableWorker = make(chan struct{}, 1)
	p.disableWorker = make(chan struct{}, 1)
	p.loopDone = make(chan struct{})
	go p.loop()
	for i := 0; i < p.maxActiveWorkers; i++ {
		w := newWorker(&p, i, p.fixedWorkers)
		p.wgWorkers.Add(1)
		go w.loop(ctxWorkers)
	}
	return &p, nil
}

type stats struct {
	Time          time.Time
	NJobsInSystem int
	Concurrency   int32
	JobID         int
	DoneCounter   int
}

func (p *Pool[I, O, C]) loop() {
	var loadAvg float64 = 1
	var jobID int
	var jobIDWhenLastEnabledWorker int
	var doneCounter int
	var doneCounterWhenLastDisabledWorker int
	var doneCounterWhenResultsFull int
	var nJobsInSystem int
	var jobDone bool
	// calculate decay factor "a"
	// of the exponentially weighted moving average
	// of loadAvg
	window := p.maxActiveWorkers / 5
	a := 2 / (float64(window) + 1)
	// window2 is used as the number of jobs that have to pass
	// before we enable or disable workers again
	window2 := 2 * window
	for p.jobsNew != nil || p.jobsDone != nil {
		concurrency := atomic.LoadInt32(&p.concurrency)
		if concurrency > 0 {
			// concurrency > 0 so we can divide
			loadNow := float64(nJobsInSystem) / float64(concurrency)
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
		if p.monitor != nil {
			p.monitor(stats{
				Time:          time.Now(),
				NJobsInSystem: nJobsInSystem,
				Concurrency:   concurrency,
				JobID:         jobID,
				DoneCounter:   doneCounter,
			})
		}
		if !p.fixedWorkers && !jobDone && loadAvg > p.targetLoad*float64(concurrency+1)/float64(concurrency) && jobID-jobIDWhenLastEnabledWorker > window2 && len(p.Results) == 0 {
			// if load is high, and we haven't enabled a worker recently, and len(p.Results) == 0, enable n workers
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
				p.enableWorkers(n)
				jobIDWhenLastEnabledWorker = jobID
			}
		}
		if !p.fixedWorkers && concurrency > 0 && jobDone {
			if loadAvg < p.targetLoad*float64(concurrency-1)/float64(concurrency) && doneCounter-doneCounterWhenLastDisabledWorker > window2 {
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
					p.disableWorkers(n)
					doneCounterWhenLastDisabledWorker = doneCounter
				}
			}
		} else if concurrency == 0 {
			loadAvg = 1
		}
		jobDone = false
		// make sure not all workers are disabled while there are jobs
		if !p.fixedWorkers && concurrency == 0 && nJobsInSystem > 0 {
			if p.loggerDebug != nil {
				p.loggerDebug.Printf("[workerpool/loop] [doneCounter=%d] no active worker. try to enable new worker", doneCounter)
			}
			p.enableWorkers(1)
		}
		if nJobsInSystem >= p.maxActiveWorkers {
			// if there are p.maxActiveWorkers jobs don't accept new jobs
			// len(p.jobsQueue) = p.maxActiveWorkers
			// that way we make sure nJobsInSystem < len(p.jobsQueue)
			// so writes to p.jobsQueue don't block
			// blocking writes to p.jobsQueue would cause deadlock
			select {
			case result, ok := <-p.jobsDone:
				if !ok {
					p.jobsDone = nil
					continue
				}
				if p.Results != nil {
					blocked := p.writeResultAndDisableWorkersIfBlocked(result, doneCounter, doneCounterWhenResultsFull, window2)
					if blocked {
						doneCounterWhenResultsFull = doneCounter
					}
				}
				nJobsInSystem--
				doneCounter++
				jobDone = true
			case _, ok := <-p.concurrencyIs0:
				if !ok {
					p.jobsNew = nil
					continue
				}
				p.enableWorkers(1)
			}
		} else {
			select {
			case payload, ok := <-p.jobsNew:
				if !ok {
					p.jobsNew = nil
					continue
				}
				nJobsInSystem++
				p.jobsQueue <- Job[I]{Payload: payload, ID: jobID, Attempt: 0}
				jobID++
			case result, ok := <-p.jobsDone:
				if !ok {
					p.jobsDone = nil
					continue
				}
				if p.Results != nil {
					blocked := p.writeResultAndDisableWorkersIfBlocked(result, doneCounter, doneCounterWhenResultsFull, window2)
					if blocked {
						doneCounterWhenResultsFull = doneCounter
					}
				}
				nJobsInSystem--
				doneCounter++
				jobDone = true
			case _, ok := <-p.concurrencyIs0:
				if !ok {
					p.jobsNew = nil
					continue
				}
				p.enableWorkers(1)
			}
		}
	}
	close(p.jobsQueue)
	if p.loggerDebug != nil {
		p.loggerDebug.Println("[workerpool/loop] finished")
	}
	p.loopDone <- struct{}{}
}

func (p *Pool[I, O, C]) writeResultAndDisableWorkersIfBlocked(result Result[I, O], doneCounter, doneCounterWhenResultsFull, window2 int) bool {
	select {
	case p.Results <- result:
		return false
	default:
		if doneCounter-doneCounterWhenResultsFull > window2 {
			// do not disable any workers if we did so recently
			concurrency := atomic.LoadInt32(&p.concurrency)
			n := int(concurrency) / 2
			if p.loggerDebug != nil {
				p.loggerDebug.Printf("[workerpool/loop] [doneCounter=%d] p.Results is full. try to disable %d workers\n", doneCounter, n)
			}
			p.disableWorkers(n)
		}
		p.Results <- result
		return true
	}
}

func (p *Pool[I, O, C]) disableWorkers(n int) {
	// drain p.enableWorker channel
loop:
	for {
		select {
		case <-p.enableWorker:
		default:
			break loop
		}
	}
	// try to disable n workers
	for i := 0; i < n; i++ {
		select {
		case p.disableWorker <- struct{}{}:
		default:
		}
	}
}

func (p *Pool[I, O, C]) enableWorkers(n int) {
	// drain p.disableWorker channel
loop:
	for {
		select {
		case <-p.disableWorker:
		default:
			break loop
		}
	}
	// try to enable n workers
	for i := 0; i < n; i++ {
		select {
		case p.enableWorker <- struct{}{}:
		default:
		}
	}
}

// Submit adds a new job to the queue.
func (p *Pool[I, O, C]) Submit(jobPayload I) {
	p.wgJobs.Add(1)
	p.jobsNew <- jobPayload
}

// StopAndWait shuts down the pool.
// Once called no more jobs can be submitted,
// and waits for all enqueued jobs to finish and workers to stop.
func (p *Pool[I, O, C]) StopAndWait() {
	close(p.jobsNew)
	if p.loggerDebug != nil {
		p.loggerDebug.Println("[workerpool/StopAndWait] waiting for all jobs to finish")
	}
	p.wgJobs.Wait()
	p.cancelWorkers()
	close(p.jobsDone)
	if p.loggerDebug != nil {
		p.loggerDebug.Println("[workerpool/StopAndWait] waiting for all workers to finish")
	}
	p.wgWorkers.Wait()
	<-p.loopDone
	close(p.enableWorker)
	close(p.disableWorker)
	close(p.concurrencyIs0)
	if p.loggerDebug != nil {
		p.loggerDebug.Println("[workerpool/StopAndWait] finished")
	}
	if p.Results != nil {
		close(p.Results)
	}
}

// ConnectPools starts a goroutine that reads the results of the first pool,
// and submits them to the second, if there is no error,
// or passes them to the handleError function if there is an error.
//
// Once you call StopAndWait() on the first pool,
// after a while p1.Results get closed,
// so this loop exits and p2.StopAndWait() is called.
// This way StopAndWait() propagates through the pipeline.
//
// WARNING: Should only be used if the first pool has a not-nil Results channel.
// Which means it was created by the constructors NewPoolWithResults() or NewPoolWithResultsAndInit().
func ConnectPools[I, O, C, O2, C2 any](p1 *Pool[I, O, C], p2 *Pool[O, O2, C2], handleError func(Result[I, O])) {
	go func() {
		for result := range p1.Results {
			if result.Error != nil {
				if handleError != nil {
					handleError(result)
				}
			} else {
				p2.Submit(result.Value)
			}
		}
		p2.StopAndWait()
	}()
}

type worker[I, O, C any] struct {
	id            int
	pool          *Pool[I, O, C]
	connection    *C
	idleTicker    *time.Ticker
	alwaysEnabled bool
}

func newWorker[I, O, C any](p *Pool[I, O, C], id int, alwaysEnabled bool) *worker[I, O, C] {
	return &worker[I, O, C]{
		id:            id,
		pool:          p,
		alwaysEnabled: alwaysEnabled,
	}
}

func (w *worker[I, O, C]) loop(ctx context.Context) {
	enabled := false
	deinit := func() {
		if w.idleTicker != nil {
			w.idleTicker.Stop()
		}
		if enabled {
			enabled = false
			atomic.AddInt32(&w.pool.concurrency, -1)
			if w.pool.workerDeinit != nil && w.connection != nil {
				err := w.pool.workerDeinit(w.id, *w.connection)
				if err != nil {
					if w.pool.loggerInfo != nil {
						w.pool.loggerInfo.Printf("[workerpool/worker%d] workerDeinit failed: %s\n", w.id, err)
					}
				}
				w.connection = nil
			}
			concurrency := atomic.LoadInt32(&w.pool.concurrency)
			if w.pool.loggerDebug != nil {
				w.pool.loggerDebug.Printf("[workerpool/worker%d] worker disabled - concurrency %d\n", w.id, concurrency)
			}
			if concurrency == 0 {
				// if all workers are disabled, the pool loop might get stuck resulting in a deadlock.
				// we send a signal to p.concurrencyIs0 so the pool loop can continue and enable one worker.
				if w.pool.loggerDebug != nil {
					w.pool.loggerDebug.Printf("[workerpool/worker%d] sending to p.concurrencyIs0\n", w.id)
				}
				select {
				case w.pool.concurrencyIs0 <- struct{}{}:
				default:
				}
			}
		}
	}
	defer w.pool.wgWorkers.Done()
	defer deinit()
loop:
	for {
		if !enabled {
			if !w.alwaysEnabled && w.idleTicker != nil {
				w.idleTicker.Stop()
			}
			if !w.alwaysEnabled {
				select {
				case _, ok := <-w.pool.enableWorker:
					if !ok {
						break loop
					}
				case <-ctx.Done():
					break loop
				}
			}
			enabled = true
			atomic.AddInt32(&w.pool.concurrency, 1)
			if w.pool.loggerDebug != nil {
				if w.pool.loggerDebug != nil {
					w.pool.loggerDebug.Printf("[workerpool/worker%d] worker enabled\n", w.id)
				}
			}
			if w.pool.workerInit != nil {
				connection, err := w.pool.workerInit(w.id)
				if err != nil {
					if w.pool.loggerInfo != nil {
						w.pool.loggerInfo.Printf("[workerpool/worker%d] workerInit failed: %s\n", w.id, err)
					}
					enabled = false
					atomic.AddInt32(&w.pool.concurrency, -1)
					time.Sleep(w.pool.reinitDelay)
					// this worker failed to start, so enable another worker
					w.pool.enableWorkers(1)
					continue
				}
				w.connection = &connection
			} else {
				w.connection = new(C)
			}
			if !w.alwaysEnabled {
				w.idleTicker = time.NewTicker(w.pool.idleTimeout)
			} else {
				neverTickingTicker := time.Ticker{C: make(chan time.Time)}
				w.idleTicker = &neverTickingTicker
			}
		}
		select {
		case <-w.idleTicker.C:
			deinit()
		case <-ctx.Done():
			break loop
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
			if !w.alwaysEnabled {
				w.idleTicker.Stop()
			}
			if err != nil && errorIsRetryable(err) && (j.Attempt < w.pool.retries || errorIsUnaccounted(err)) {
				if !errorIsUnaccounted(err) {
					j.Attempt++
				}
				w.pool.jobsQueue <- j
			} else {
				w.pool.jobsDone <- Result[I, O]{Job: j, Value: resultValue, Error: err}
				w.pool.wgJobs.Done()
			}
			if err != nil {
				pauseDuration := errorPausesWorker(err)
				if pauseDuration > 0 {
					deinit()
					w.pool.enableWorkers(1)
					ctxCanceled := sleepCtx(ctx, pauseDuration)
					if ctxCanceled {
						break loop
					}
				}
			}
			if !w.alwaysEnabled {
				w.idleTicker = time.NewTicker(w.pool.idleTimeout)
			}
		}
	}
	if w.pool.loggerDebug != nil {
		w.pool.loggerDebug.Printf("[workerpool/worker%d] finished\n", w.id)
	}
}

func sleepCtx(ctx context.Context, dur time.Duration) bool {
	ticker := time.NewTicker(dur)
	defer ticker.Stop()
	select {
	case <-ctx.Done():
		return true
	case <-ticker.C:
		return false
	}
}
