// Copyright (C) 2022 Charalampos Mitsakis (go.mitsakis.org/workerpool)
// Licensed under the Apache License, Version 2.0

package workerpool

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"
)

var (
	flagDebugLogs           = flag.Bool("debug", false, "Enable debug logs")
	flagSaveTimeseriesToDir = flag.String("save-timeseries-dir", "", "Save concurrency timeseries data to files in the given directory")
)

func TestExample(t *testing.T) {
	p, _ := NewPoolSimple(4, func(job Job[float64], workerID int) error {
		result := math.Sqrt(job.Payload)
		t.Logf("result: %v", result)
		return nil
	}, LoggerInfo(loggerIfDebugEnabled()), LoggerDebug(loggerIfDebugEnabled()))
	for i := 0; i < 100; i++ {
		p.Submit(float64(i))
	}
	p.StopAndWait()
}

func TestPoolCorrectness(t *testing.T) {
	p, err := NewPoolWithResults(5, func(job Job[float64], workerID int) (float64, error) {
		// fail the first attempt only
		if job.Attempt == 0 {
			return 0, ErrorWrapRetryable(fmt.Errorf("failed"))
		}
		return math.Sqrt(job.Payload), nil
	}, Retries(1), Name("p"), LoggerInfo(loggerIfDebugEnabled()), LoggerDebug(loggerIfDebugEnabled()))
	if err != nil {
		t.Errorf("[ERROR] failed to create pool p: %s", err)
		return
	}

	const submittedCount = 100
	go func() {
		for i := 0; i < submittedCount; i++ {
			p.Submit(float64(i))
		}
		p.StopAndWait()
	}()

	var resultsCount int
	seenPayloads := make(map[float64]struct{}, submittedCount)
	for result := range p.Results {
		if result.Error != nil {
			t.Errorf("[ERROR] result contains error: %v", result.Error)
		}
		resultsCount++
		if result.Value != math.Sqrt(result.Job.Payload) {
			t.Errorf("[ERROR] wrong result: job.Payload=%v result.Value=%v", result.Job.Payload, result.Value)
		}
		if _, exists := seenPayloads[result.Job.Payload]; exists {
			t.Errorf("[ERROR] duplicate job.Payload=%v", result.Job.Payload)
		}
		seenPayloads[result.Job.Payload] = struct{}{}
	}
	if resultsCount != submittedCount {
		t.Error("[ERROR] submittedCount != resultsCount")
	}
}

// Failure does not mean there is an error, but that the auto-scaling behavior of the pool is not ideal and can be improved.
func TestPoolAutoscalingBehavior(t *testing.T) {
	var logger *log.Logger
	if *flagDebugLogs {
		logger = log.New(os.Stdout, "[DEBUG] [test] ", log.LstdFlags|log.Lmsgprefix)
	} else {
		logger = log.New(io.Discard, "", 0)
	}
	rand.Seed(time.Now().UnixNano())
	workerProfiles := make([]string, 0)
	for i := 0; i < 100; i++ {
		workerProfiles = append(workerProfiles, fmt.Sprintf("w%d", i))
	}
	const inputPeriod = 10 * time.Millisecond
	const jobDur = 500 * time.Millisecond
	const successRate = 0.75
	var pStats []stats
	results := make(chan struct{})
	p, err := NewPoolWithInit(len(workerProfiles), func(job Job[int], workerID int, connection struct{}) error {
		worker := workerProfiles[workerID]
		logger.Printf("[worker%v] job%d started - attempt %d - worker %v\n", workerID, job.ID, job.Attempt, worker)
		time.Sleep(jobDur)
		if rand.Float32() > successRate {
			return ErrorWrapRetryableUnaccounted(fmt.Errorf("job failure"))
		}
		results <- struct{}{}
		return nil
	}, func(workerID int) (struct{}, error) {
		time.Sleep(3 * jobDur)
		if rand.Float32() > 0.9 {
			return struct{}{}, fmt.Errorf("worker init failure")
		}
		logger.Printf("[worker%v] connecting\n", workerID)
		return struct{}{}, nil
	}, func(workerID int, connection struct{}) error {
		time.Sleep(3 * jobDur)
		if rand.Float32() > 0.9 {
			return fmt.Errorf("worker deinit failure")
		}
		logger.Printf("[worker%v] disconnecting\n", workerID)
		return nil
	}, LoggerInfo(loggerIfDebugEnabled()), LoggerDebug(loggerIfDebugEnabled()), monitor(func(s stats) {
		pStats = append(pStats, s)
	}))
	if err != nil {
		t.Errorf("[ERROR] failed to create pool: %s", err)
		return
	}
	started := time.Now()
	var stopped time.Time
	var submittedCount int
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()
		i := 0
		for ; i < 100000; i++ {
			if sleepCtx(ctx, inputPeriod) {
				break
			}
			logger.Printf("submitting job%d\n", i)
			p.Submit(i)
		}
		logger.Printf("submitted %d jobs - calling p.StopAndWait()\n", i)
		t.Logf("[INFO] submitted %d jobs\n", i)
		submittedCount = i
		stopped = time.Now()
		p.StopAndWait()
		logger.Println("p.StopAndWait() returned")
		close(results)
	}()
	const a = 0.1
	var outputPeriodAVG time.Duration
	lastReceived := time.Now()
	var resultsCount int
	for range results {
		outputPeriod := time.Since(lastReceived)
		lastReceived = time.Now()
		outputPeriodAVG = time.Duration(a*float64(outputPeriod) + (1-a)*float64(outputPeriodAVG))
		logger.Println("outputPeriodAVG:", outputPeriodAVG)
		resultsCount++
	}
	t.Logf("[INFO] got %d results\n", resultsCount)
	if submittedCount != resultsCount {
		t.Error("[ERROR] submittedCount != resultsCount")
	}

	if *flagSaveTimeseriesToDir != "" {
		err := saveConcurrencyStatsToFile(pStats, *flagSaveTimeseriesToDir+"/"+t.Name()+".txt")
		if err != nil {
			t.Logf("saveConcurrencyStatsToFile failed: %v", err)
		}
	}

	pWorkersAVG, pWorkersSD, throughput := processStats(pStats, started.Add(30*time.Second), stopped)
	t.Logf("[INFO] pool workers: AVG=%v SD=%v\n", pWorkersAVG, pWorkersSD)
	// expectedNumOfWorkers = effectiveJobDur/inputPeriod
	// where effectiveJobDur = jobDur / successRate
	// because each job is tried 1/successRate on average
	expectedNumOfWorkers := float64(jobDur/inputPeriod) / successRate
	if pWorkersAVG < 0.95*expectedNumOfWorkers {
		t.Errorf("[WARNING] pWorkersAVG < 0.95*%v", expectedNumOfWorkers)
	}
	if pWorkersAVG > 1.1*expectedNumOfWorkers {
		t.Errorf("[WARNING] pWorkersAVG > 1.1*%v", expectedNumOfWorkers)
	}
	// fail if standard deviation is too high
	if pWorkersSD/pWorkersAVG > 0.1 {
		t.Error("[WARNING] pWorkersSD/pWorkersAVG > 0.1")
	}

	t.Logf("[INFO] throughput: %v\n", throughput)
	// expectedThroughput calculation assumes the inputPeriod is long enough that there is no backpressure
	inputPeriodInSeconds := float64(inputPeriod) / float64(time.Second)
	expectedThroughput := 1 / inputPeriodInSeconds
	if throughput < 0.85*expectedThroughput {
		t.Errorf("[WARNING] throughput < %v", 0.85*expectedThroughput)
	}
	if throughput > 1.1*expectedThroughput {
		t.Errorf("[WARNING] throughput > %v", 1.1*expectedThroughput)
	}

	throughputPerWorker := throughput / pWorkersAVG
	t.Logf("[INFO] throughputPerWorker: %v\n", throughputPerWorker)
	// expectedThroughputPerWorker calculation assumes the inputPeriod is long enough that there is no backpressure
	expectedThroughputPerWorker := expectedThroughput / expectedNumOfWorkers
	if throughputPerWorker < 0.85*expectedThroughputPerWorker {
		t.Errorf("[WARNING] throughputPerWorker < %v", 0.85*expectedThroughputPerWorker)
	}
	if throughputPerWorker > 1.1*expectedThroughputPerWorker {
		t.Errorf("[WARNING] throughputPerWorker > %v", 1.1*expectedThroughputPerWorker)
	}
}

type pair[V, P any] struct {
	Value           V
	OriginalPayload P
}

func TestPipelineCorrectness(t *testing.T) {
	// stage 1: calculate square root
	p1, err := NewPoolWithResults(5, func(job Job[int], workerID int) (pair[float64, int], error) {
		// fail the first attempt only
		if job.Attempt == 0 {
			return pair[float64, int]{0, 0}, ErrorWrapRetryable(fmt.Errorf("failed"))
		}
		return pair[float64, int]{Value: math.Sqrt(float64(job.Payload)), OriginalPayload: job.Payload}, nil
	}, Retries(1), Name("p1"), LoggerInfo(loggerIfDebugEnabled()), LoggerDebug(loggerIfDebugEnabled()))
	if err != nil {
		t.Errorf("[ERROR] failed to create pool p1: %s", err)
		return
	}

	// stage 2: negate number
	p2, err := NewPoolWithResults(5, func(job Job[pair[float64, int]], workerID int) (pair[float64, int], error) {
		return pair[float64, int]{Value: -job.Payload.Value, OriginalPayload: job.Payload.OriginalPayload}, nil
	}, Name("p2"), LoggerInfo(loggerIfDebugEnabled()), LoggerDebug(loggerIfDebugEnabled()))
	if err != nil {
		t.Errorf("[ERROR] failed to create pool p2: %s", err)
		return
	}

	// stage 3: convert float to string
	p3, err := NewPoolWithResults(5, func(job Job[pair[float64, int]], workerID int) (string, error) {
		return fmt.Sprintf("%.3f", job.Payload.Value), nil
	}, Name("p3"), LoggerInfo(loggerIfDebugEnabled()), LoggerDebug(loggerIfDebugEnabled()))
	if err != nil {
		t.Errorf("[ERROR] failed to create pool p3: %s", err)
		return
	}

	ConnectPools(p1, p2, nil)
	ConnectPools(p2, p3, nil)

	const submittedCount = 100
	go func() {
		for i := 0; i < submittedCount; i++ {
			p1.Submit(i)
		}
		p1.StopAndWait()
	}()

	var resultsCount int
	seenPayloads := make(map[int]struct{}, submittedCount)
	for result := range p3.Results {
		if result.Error != nil {
			t.Errorf("[ERROR] result contains error: %v", result.Error)
		}
		resultsCount++
		if result.Value != fmt.Sprintf("%.3f", -math.Sqrt(float64(result.Job.Payload.OriginalPayload))) {
			t.Errorf("[ERROR] wrong result: OriginalPayload=%v result.Value=%v", result.Job.Payload.OriginalPayload, result.Value)
		}
		if _, exists := seenPayloads[result.Job.Payload.OriginalPayload]; exists {
			t.Errorf("[ERROR] duplicate job.Payload=%v", result.Job.Payload.OriginalPayload)
		}
		seenPayloads[result.Job.Payload.OriginalPayload] = struct{}{}
	}
	if resultsCount != submittedCount {
		t.Error("[ERROR] submittedCount != resultsCount")
	}
}

// Failure does not mean there is an error, but that the auto-scaling behavior of the pools is not ideal and can be improved.
// Use: 'go test -timeout 30m -v' to make sure all tests run, and be able to read all the stats.
// Alternatively use: 'go test -short -v' to run a small number of test cases.
func TestPipelineAutoscalingBehavior(t *testing.T) {
	var numOfWorkersSlice []int
	if testing.Short() == true {
		numOfWorkersSlice = []int{10, 100}
	} else {
		numOfWorkersSlice = []int{5, 10, 20, 50, 100, 200, 500}
	}
	var inputPeriodSlice []time.Duration
	if testing.Short() == true {
		inputPeriodSlice = []time.Duration{0}
	} else {
		inputPeriodSlice = []time.Duration{20 * time.Millisecond, 10 * time.Millisecond, 0}
	}

	var resultsCountSum int
	var workersNumErrorSum float64
	var workersNumRSDSum float64
	var throughputPerActivationFractionSum float64
	for _, w := range numOfWorkersSlice {
		for _, p := range inputPeriodSlice {
			t.Run(fmt.Sprintf("w=%v_p=%v", w, p), func(t *testing.T) {
				resultsCount, workersNumError, workersNumRSD, throughput, activationFraction := testPipelineAutoscalingBehaviorCase(t, w, p)
				resultsCountSum += resultsCount
				workersNumErrorSum += workersNumError
				workersNumRSDSum += workersNumRSD
				// throughputPerActivationFraction:
				// throughput divided by activationFraction (fraction of workers that are activated)
				// in order to normalize the values of all tests and make them comparable.
				// if job duration was constant, we would divide by number of active workers,
				// but since we define job duration to be proportional to the number of workers,
				// we have to divide by (number of active workers) / (number of workers) = activationFraction
				throughputPerActivationFraction := float64(throughput) / activationFraction
				throughputPerActivationFractionSum += throughputPerActivationFraction
			})
		}
	}
	numOfTests := len(numOfWorkersSlice) * len(inputPeriodSlice)
	resultsCountAVG := resultsCountSum / numOfTests
	workersNumErrorAVG := workersNumErrorSum / float64(numOfTests)
	workersNumRSDAVG := workersNumRSDSum / float64(numOfTests)
	throughputPerActivationFractionAVG := throughputPerActivationFractionSum / float64(numOfTests)
	t.Logf("[INFO] resultsCount average: %v", resultsCountAVG)
	t.Logf("[INFO] workersNumError average: %v", workersNumErrorAVG)
	t.Logf("[INFO] workersNumRSD average: %v", workersNumRSDAVG)
	t.Logf("[INFO] throughputPerActivationFraction average: %v", throughputPerActivationFractionAVG)
}

func testPipelineAutoscalingBehaviorCase(t *testing.T, numOfWorkers int, inputPeriod time.Duration) (int, float64, float64, float64, float64) {
	var logger *log.Logger
	if *flagDebugLogs {
		logger = log.New(os.Stdout, "[DEBUG] [test] ", log.LstdFlags|log.Lmsgprefix)
	} else {
		logger = log.New(io.Discard, "", 0)
	}

	jobDur1 := 3333 * time.Duration(numOfWorkers) * time.Microsecond
	jobDur2 := 6666 * time.Duration(numOfWorkers) * time.Microsecond
	jobDur3 := 10000 * time.Duration(numOfWorkers) * time.Microsecond

	var p1Stats []stats
	var p2Stats []stats
	var p3Stats []stats

	// stage 1: calculate square root
	p1, err := NewPoolWithResults(numOfWorkers, func(job Job[float64], workerID int) (float64, error) {
		time.Sleep(jobDur1)
		return math.Sqrt(job.Payload), nil
	}, Name("p1"), LoggerInfo(loggerIfDebugEnabled()), LoggerDebug(loggerIfDebugEnabled()), monitor(func(s stats) {
		p1Stats = append(p1Stats, s)
	}))
	if err != nil {
		t.Errorf("[ERROR] failed to create pool p1: %s", err)
		return 0, math.NaN(), math.NaN(), math.NaN(), math.NaN()
	}

	// stage 2: negate number
	p2, err := NewPoolWithResults(numOfWorkers, func(job Job[float64], workerID int) (float64, error) {
		time.Sleep(jobDur2)
		return -job.Payload, nil
	}, Name("p2"), LoggerInfo(loggerIfDebugEnabled()), LoggerDebug(loggerIfDebugEnabled()), monitor(func(s stats) {
		p2Stats = append(p2Stats, s)
	}))
	if err != nil {
		t.Errorf("[ERROR] failed to create pool p2: %s", err)
		return 0, math.NaN(), math.NaN(), math.NaN(), math.NaN()
	}

	// stage 3: convert float to string
	p3, err := NewPoolWithResults(numOfWorkers, func(job Job[float64], workerID int) (string, error) {
		time.Sleep(jobDur3)
		return fmt.Sprintf("%.3f", job.Payload), nil
	}, Name("p3"), LoggerInfo(loggerIfDebugEnabled()), LoggerDebug(loggerIfDebugEnabled()), monitor(func(s stats) {
		p3Stats = append(p3Stats, s)
	}))
	if err != nil {
		t.Errorf("[ERROR] failed to create pool p3: %s", err)
		return 0, math.NaN(), math.NaN(), math.NaN(), math.NaN()
	}

	// connect p1, p2, p3 into a pipeline
	ConnectPools(p1, p2, nil)
	ConnectPools(p2, p3, nil)

	const a = 0.1
	var inputPeriodAVG time.Duration
	lastSubmitted := time.Now()
	started := time.Now()
	var submittedCount int
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()
		i := 0
	loop:
		for ; i < 10000; i++ {
			if inputPeriod == 0 {
				select {
				case <-ctx.Done():
					break loop
				default:
				}
			} else {
				if sleepCtx(ctx, inputPeriod) {
					break
				}
			}
			logger.Printf("submitting job%d - inputPeriodAVG: %v\n", i, inputPeriodAVG)
			p1.Submit(float64(i))
			inputPeriodNow := time.Since(lastSubmitted)
			lastSubmitted = time.Now()
			inputPeriodAVG = time.Duration(a*float64(inputPeriodNow) + (1-a)*float64(inputPeriodAVG))
		}
		logger.Printf("submitted %d jobs - calling p.StopAndWait()\n", i)
		t.Logf("[INFO] submitted %d jobs\n", i)
		submittedCount = i
		p1.StopAndWait()
	}()

	var outputPeriodAVG time.Duration
	lastReceived := time.Now()
	var resultsCount int
	for result := range p3.Results {
		outputPeriod := time.Since(lastReceived)
		lastReceived = time.Now()
		outputPeriodAVG = time.Duration(a*float64(outputPeriod) + (1-a)*float64(outputPeriodAVG))
		logger.Println("result:", result.Value, "outputPeriodAVG:", outputPeriodAVG)
		resultsCount++
	}
	if submittedCount != resultsCount {
		t.Error("[ERROR] submittedCount != resultsCount")
	}

	if *flagSaveTimeseriesToDir != "" {
		repFunc := func(r rune) rune {
			if r == '/' || r == '=' {
				return '_'
			}
			return r
		}
		tNameSafe := strings.Map(repFunc, t.Name())
		err := saveConcurrencyStatsToFile(p1Stats, *flagSaveTimeseriesToDir+"/"+tNameSafe+"_p1.txt")
		if err != nil {
			t.Logf("saveConcurrencyStatsToFile failed: %v", err)
		}
		err = saveConcurrencyStatsToFile(p2Stats, *flagSaveTimeseriesToDir+"/"+tNameSafe+"_p2.txt")
		if err != nil {
			t.Logf("saveConcurrencyStatsToFile failed: %v", err)
		}
		err = saveConcurrencyStatsToFile(p3Stats, *flagSaveTimeseriesToDir+"/"+tNameSafe+"_p3.txt")
		if err != nil {
			t.Logf("saveConcurrencyStatsToFile failed: %v", err)
		}
	}

	p1WorkersAVG, p1WorkersSD, _ := processStats(p1Stats, started.Add(30*time.Second), lastSubmitted)
	p2WorkersAVG, p2WorkersSD, _ := processStats(p2Stats, started.Add(30*time.Second), lastSubmitted)
	p3WorkersAVG, p3WorkersSD, throughput := processStats(p3Stats, started.Add(30*time.Second), lastSubmitted)
	t.Logf("[INFO] [pool=p1] workers: AVG=%v SD=%v\n", p1WorkersAVG, p1WorkersSD)
	t.Logf("[INFO] [pool=p2] workers: AVG=%v SD=%v\n", p2WorkersAVG, p2WorkersSD)
	t.Logf("[INFO] [pool=p3] workers: AVG=%v SD=%v\n", p3WorkersAVG, p3WorkersSD)

	// p1WorkersAVG should be about 1/3 of p3WorkersAVG
	p1WorkersExpected := 0.3333 * p3WorkersAVG
	if p1WorkersAVG < 0.8*p1WorkersExpected-1 {
		t.Errorf("[WARNING] p1WorkersAVG < %v", 0.8*p1WorkersExpected-1)
	}
	if p1WorkersAVG > 1.2*p1WorkersExpected+1 {
		t.Errorf("[WARNING] p1WorkersAVG > %v", 1.2*p1WorkersExpected+1)
	}
	// p2WorkersAVG should be about 2/3 of p3WorkersAVG
	p2WorkersExpected := 0.6666 * p3WorkersAVG
	if p2WorkersAVG < 0.8*p2WorkersExpected-1 {
		t.Errorf("[WARNING] p2WorkersAVG < %v", 0.8*p2WorkersExpected-1)
	}
	if p2WorkersAVG > 1.2*p2WorkersExpected+1 {
		t.Errorf("[WARNING] p2WorkersAVG > %v", 1.2*p2WorkersExpected+1)
	}
	// p3WorkersAVG should be about p3WorkersExpected
	var p3WorkersExpected float64
	if inputPeriod > 0 {
		p3WorkersExpected = float64(jobDur3 / inputPeriod)
		if p3WorkersExpected > float64(numOfWorkers) {
			p3WorkersExpected = float64(numOfWorkers)
		}
	} else {
		p3WorkersExpected = float64(numOfWorkers)
	}
	if p3WorkersAVG < 0.9*p3WorkersExpected-1 {
		t.Errorf("[WARNING] p3WorkersAVG < %v", 0.9*p3WorkersExpected-1)
	}
	if p3WorkersAVG > 1.1*p3WorkersExpected+1 {
		t.Errorf("[WARNING] p3WorkersAVG > %v", 1.1*p3WorkersExpected+1)
	}

	// fail if standard deviation is too high
	if p1WorkersSD/p1WorkersAVG > 0.1 && p1WorkersSD > 1 {
		t.Error("[WARNING] p1WorkersSD too high")
	}
	if p2WorkersSD/p2WorkersAVG > 0.1 && p2WorkersSD > 1 {
		t.Error("[WARNING] p2WorkersSD too high")
	}
	if p3WorkersSD/p3WorkersAVG > 0.05 && p3WorkersSD > 1 {
		t.Error("[WARNING] p3WorkersSD too high")
	}

	workersNumError := math.Sqrt(math.Pow(p1WorkersAVG-p1WorkersExpected, 2)+math.Pow(p2WorkersAVG-p2WorkersExpected, 2)+math.Pow(p3WorkersAVG-p3WorkersExpected, 2)) / p3WorkersExpected
	workersNumRSD := p1WorkersSD/p1WorkersAVG + p2WorkersSD/p2WorkersAVG + p3WorkersSD/p3WorkersAVG
	activationFraction := (p1WorkersAVG + p2WorkersAVG + p3WorkersAVG) / float64(3*numOfWorkers)
	t.Logf("[INFO] workersNumError: %v", workersNumError)
	t.Logf("[INFO] workersNumRSD: %v", workersNumRSD)
	t.Logf("[INFO] throughput: %v", throughput)
	t.Logf("[INFO] activationFraction: %v", activationFraction)
	t.Logf("[INFO] throughputPerActivationFraction: %v", float64(throughput)/activationFraction)
	return resultsCount, workersNumError, workersNumRSD, throughput, activationFraction
}

// calculates the average and standard deviation of concurrency in the specified time period
func processStats(statsArray []stats, from time.Time, to time.Time) (float64, float64, float64) {
	var workersSum int
	var workersSumSq int

	// number of elements of statsArray within the specified time period
	var n int

	// time at the first element of statsArray within the specified time period
	var t0 time.Time
	// time at the last element of statsArray within the specified time period
	var t1 time.Time

	// doneCounter at the first element of statsArray within the specified time period
	var doneCounter0 int
	// doneCounter at the last element of statsArray within the specified time period
	var doneCounter1 int

	first := true
	for _, s := range statsArray {
		if s.Time.Before(from) {
			continue
		} else if s.Time.After(to) {
			break
		}
		n++
		if first {
			first = false
			t0 = s.Time
			doneCounter0 = s.DoneCounter
		}
		t1 = s.Time
		doneCounter1 = s.DoneCounter
		workersSum += int(s.Concurrency)
		workersSumSq += int(s.Concurrency * s.Concurrency)
	}
	nFloat := float64(n)
	workersAVG := float64(workersSum) / nFloat
	workersSD := math.Sqrt(float64(workersSumSq)/nFloat - math.Pow(float64(workersSum)/nFloat, 2))
	numOfJobsDone := doneCounter1 - doneCounter0
	dtInSeconds := float64(t1.Sub(t0)) / float64(time.Second)
	throughput := float64(numOfJobsDone) / dtInSeconds
	return workersAVG, workersSD, throughput
}

func saveConcurrencyStatsToFile(statsArray []stats, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("os.Create() failed: %v", err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)

	var t0 time.Time
	first := true
	for _, s := range statsArray {
		if first {
			first = false
			t0 = s.Time
		}
		dt := s.Time.Sub(t0)
		_, err = w.WriteString(fmt.Sprintf("%v %v\n", dt.Microseconds(), s.Concurrency))
		if err != nil {
			return fmt.Errorf("w.WriteString() failed: %v", err)
		}
	}

	err = w.Flush()
	if err != nil {
		return fmt.Errorf("w.Flush() failed: %v", err)
	}
	return nil
}

func loggerIfDebugEnabled() *log.Logger {
	if *flagDebugLogs {
		return log.New(os.Stdout, "[DEBUG] ", log.LstdFlags|log.Lmsgprefix)
	}
	return nil
}
