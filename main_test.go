// Copyright (C) 2022 Charalampos Mitsakis (go.mitsakis.org/workerpool)
// Licensed under the Apache License, Version 2.0

package workerpool

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"testing"
	"time"
)

var flagDebugLogs = flag.Bool("debug", false, "Enable debug logs")

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

func TestPool(t *testing.T) {
	var logger *log.Logger
	if *flagDebugLogs {
		logger = log.Default()
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
		logger.Printf("[test/worker%v] job%d started - attempt %d - worker %v\n", workerID, job.ID, job.Attempt, worker)
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
		logger.Printf("[test/worker%v] connecting\n", workerID)
		return struct{}{}, nil
	}, func(workerID int, connection struct{}) error {
		time.Sleep(3 * jobDur)
		if rand.Float32() > 0.9 {
			return fmt.Errorf("worker deinit failure")
		}
		logger.Printf("[test/worker%v] disconnecting\n", workerID)
		return nil
	}, LoggerInfo(loggerIfDebugEnabled()), LoggerDebug(loggerIfDebugEnabled()), monitor(func(s stats) {
		pStats = append(pStats, s)
	}))
	if err != nil {
		t.Errorf("failed to create pool: %s", err)
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
			logger.Printf("[test] submitting job%d\n", i)
			p.Submit(i)
		}
		logger.Printf("[test] submitted %d jobs - calling p.StopAndWait()\n", i)
		t.Logf("submitted %d jobs\n", i)
		submittedCount = i
		stopped = time.Now()
		p.StopAndWait()
		logger.Println("[test] p.StopAndWait() returned")
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
		logger.Println("[test] outputPeriodAVG:", outputPeriodAVG)
		resultsCount++
	}
	t.Logf("got %d results\n", resultsCount)
	if submittedCount != resultsCount {
		t.Error("submittedCount != resultsCount")
	}

	pWorkersAVG, pWorkersSD := processStats(pStats, started.Add(30*time.Second), stopped)
	t.Logf("pool workers: AVG=%v SD=%v\n", pWorkersAVG, pWorkersSD)
	// expectedNumOfWorkers = effectiveJobDur/inputPeriod
	// where effectiveJobDur = jobDur / successRate
	// because each job is tried 1/successRate on average
	expectedNumOfWorkers := float64(jobDur/inputPeriod) / successRate
	if pWorkersAVG < 0.95*expectedNumOfWorkers {
		t.Errorf("pWorkersAVG < 0.95*%v", expectedNumOfWorkers)
	}
	if pWorkersAVG > 1.1*expectedNumOfWorkers {
		t.Errorf("pWorkersAVG > 1.1*%v", expectedNumOfWorkers)
	}
	// fail if standard deviation is too high
	if pWorkersSD/pWorkersAVG > 0.1 {
		t.Error("pWorkersSD/pWorkersAVG > 0.1")
	}
}

func TestPipeline(t *testing.T) {
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
	for _, w := range numOfWorkersSlice {
		for _, p := range inputPeriodSlice {
			t.Run(fmt.Sprintf("w=%v_p=%v", w, p), func(t *testing.T) {
				resultsCount, workersNumError, workersNumRSD := testPipelineCase(t, w, p)
				resultsCountSum += resultsCount
				workersNumErrorSum += workersNumError
				workersNumRSDSum += workersNumRSD
			})
		}
	}
	resultsCountAVG := resultsCountSum / (len(numOfWorkersSlice) * len(inputPeriodSlice))
	workersNumErrorAVG := workersNumErrorSum / float64(len(numOfWorkersSlice) * len(inputPeriodSlice))
	workersNumRSDAVG := workersNumRSDSum / float64(len(numOfWorkersSlice) * len(inputPeriodSlice))
	t.Logf("resultsCount average: %v", resultsCountAVG)
	t.Logf("workersNumError average: %v", workersNumErrorAVG)
	t.Logf("workersNumRSD average: %v", workersNumRSDAVG)
}

func testPipelineCase(t *testing.T, numOfWorkers int, inputPeriod time.Duration) (int, float64, float64) {
	var logger *log.Logger
	if *flagDebugLogs {
		logger = log.Default()
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
		t.Errorf("failed to create pool p1: %s", err)
		return 0, math.NaN(), math.NaN()
	}

	// stage 2: negate number
	p2, err := NewPoolWithResults(numOfWorkers, func(job Job[float64], workerID int) (float64, error) {
		time.Sleep(jobDur2)
		return -job.Payload, nil
	}, Name("p2"), LoggerInfo(loggerIfDebugEnabled()), LoggerDebug(loggerIfDebugEnabled()), monitor(func(s stats) {
		p2Stats = append(p2Stats, s)
	}))
	if err != nil {
		t.Errorf("failed to create pool p2: %s", err)
		return 0, math.NaN(), math.NaN()
	}

	// stage 3: convert float to string
	p3, err := NewPoolWithResults(numOfWorkers, func(job Job[float64], workerID int) (string, error) {
		time.Sleep(jobDur3)
		return fmt.Sprintf("%.3f", job.Payload), nil
	}, Name("p3"), LoggerInfo(loggerIfDebugEnabled()), LoggerDebug(loggerIfDebugEnabled()), monitor(func(s stats) {
		p3Stats = append(p3Stats, s)
	}))
	if err != nil {
		t.Errorf("failed to create pool p3: %s", err)
		return 0, math.NaN(), math.NaN()
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
			logger.Printf("[test] submitting job%d - inputPeriodAVG: %v\n", i, inputPeriodAVG)
			p1.Submit(float64(i))
			inputPeriodNow := time.Since(lastSubmitted)
			lastSubmitted = time.Now()
			inputPeriodAVG = time.Duration(a*float64(inputPeriodNow) + (1-a)*float64(inputPeriodAVG))
		}
		logger.Printf("[test] submitted %d jobs - calling p.StopAndWait()\n", i)
		t.Logf("submitted %d jobs\n", i)
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
		logger.Println("[test] result:", result.Value, "outputPeriodAVG:", outputPeriodAVG)
		resultsCount++
	}
	if submittedCount != resultsCount {
		t.Error("submittedCount != resultsCount")
	}

	p1WorkersAVG, p1WorkersSD := processStats(p1Stats, started.Add(30*time.Second), lastSubmitted)
	p2WorkersAVG, p2WorkersSD := processStats(p2Stats, started.Add(30*time.Second), lastSubmitted)
	p3WorkersAVG, p3WorkersSD := processStats(p3Stats, started.Add(30*time.Second), lastSubmitted)
	t.Logf("[pool=p1] workers: AVG=%v SD=%v\n", p1WorkersAVG, p1WorkersSD)
	t.Logf("[pool=p2] workers: AVG=%v SD=%v\n", p2WorkersAVG, p2WorkersSD)
	t.Logf("[pool=p3] workers: AVG=%v SD=%v\n", p3WorkersAVG, p3WorkersSD)

	// p1WorkersAVG should be about 1/3 of p3WorkersAVG
	p1WorkersExpected := 0.3333 * p3WorkersAVG
	if p1WorkersAVG < 0.8*p1WorkersExpected-1 {
		t.Errorf("p1WorkersAVG < %v", 0.8*p1WorkersExpected-1)
	}
	if p1WorkersAVG > 1.2*p1WorkersExpected+1 {
		t.Errorf("p1WorkersAVG > %v", 1.2*p1WorkersExpected+1)
	}
	// p2WorkersAVG should be about 2/3 of p3WorkersAVG
	p2WorkersExpected := 0.6666 * p3WorkersAVG
	if p2WorkersAVG < 0.8*p2WorkersExpected-1 {
		t.Errorf("p2WorkersAVG < %v", 0.8*p2WorkersExpected-1)
	}
	if p2WorkersAVG > 1.2*p2WorkersExpected+1 {
		t.Errorf("p2WorkersAVG > %v", 1.2*p2WorkersExpected+1)
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
		t.Errorf("p3WorkersAVG < %v", 0.9*p3WorkersExpected-1)
	}
	if p3WorkersAVG > 1.1*p3WorkersExpected+1 {
		t.Errorf("p3WorkersAVG > %v", 1.1*p3WorkersExpected+1)
	}

	// fail if standard deviation is too high
	if p1WorkersSD/p1WorkersAVG > 0.1 && p1WorkersSD > 1 {
		t.Error("p1WorkersSD too high")
	}
	if p2WorkersSD/p2WorkersAVG > 0.1 && p2WorkersSD > 1 {
		t.Error("p2WorkersSD too high")
	}
	if p3WorkersSD/p3WorkersAVG > 0.05 && p3WorkersSD > 1 {
		t.Error("p3WorkersSD too high")
	}

	workersNumError := math.Sqrt(math.Pow(p1WorkersAVG-p1WorkersExpected, 2)+math.Pow(p2WorkersAVG-p2WorkersExpected, 2)+math.Pow(p3WorkersAVG-p3WorkersExpected, 2)) / p3WorkersExpected
	workersNumRSD := p1WorkersSD/p1WorkersAVG + p2WorkersSD/p2WorkersAVG + p3WorkersSD/p3WorkersAVG
	t.Logf("workersNumError: %v", workersNumError)
	t.Logf("workersNumRSD: %v", workersNumRSD)
	return resultsCount, workersNumError, workersNumRSD
}

// calculates the average and standard deviation of concurrency in the specified time period
func processStats(statsArray []stats, from time.Time, to time.Time) (float64, float64) {
	var workersSum int
	var workersSumSq int
	var n int
	for _, s := range statsArray {
		if s.Time.Before(from) {
			continue
		} else if s.Time.After(to) {
			break
		}
		n++
		workersSum += int(s.Concurrency)
		workersSumSq += int(s.Concurrency * s.Concurrency)
	}
	nFloat := float64(n)
	workersAVG := float64(workersSum) / nFloat
	workersSD := math.Sqrt(float64(workersSumSq)/nFloat - math.Pow(float64(workersSum)/nFloat, 2))
	return workersAVG, workersSD
}

func loggerIfDebugEnabled() *log.Logger {
	if *flagDebugLogs {
		return log.Default()
	}
	return nil
}
