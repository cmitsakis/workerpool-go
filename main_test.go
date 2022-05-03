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
	var outputPeriodAvg time.Duration
	lastReceived := time.Now()
	var resultsCount int
	for range results {
		outputPeriod := time.Since(lastReceived)
		lastReceived = time.Now()
		outputPeriodAvg = time.Duration(a*float64(outputPeriod) + (1-a)*float64(outputPeriodAvg))
		logger.Println("[test] outputPeriodAvg:", outputPeriodAvg)
		resultsCount++
	}
	t.Logf("got %d results\n", resultsCount)
	if submittedCount != resultsCount {
		t.Error("submittedCount != resultsCount")
	}

	pWorkersAvg, pWorkersStd := processStats(pStats, started.Add(30*time.Second), stopped)
	t.Logf("pool workers: avg=%v std=%v\n", pWorkersAvg, pWorkersStd)
	// expectedNumOfWorkers = effectiveJobDur/inputPeriod
	// where effectiveJobDur = jobDur / successRate
	// because each job is tried 1/successRate on average
	expectedNumOfWorkers := float64(jobDur/inputPeriod) / successRate
	if pWorkersAvg < 0.95*expectedNumOfWorkers {
		t.Errorf("pWorkersAvg < 0.95*%v", expectedNumOfWorkers)
	}
	if pWorkersAvg > 1.1*expectedNumOfWorkers {
		t.Errorf("pWorkersAvg > 1.1*%v", expectedNumOfWorkers)
	}
	// fail if standard deviation is too high
	if pWorkersStd/pWorkersAvg > 0.1 {
		t.Error("pWorkersStd/pWorkersAvg > 0.1")
	}
}

func TestPipeline10WorkersLongInputPeriod(t *testing.T) {
	testPipeline(t, 20*time.Millisecond, 10)
}

func TestPipeline10WorkersMediumInputPeriod(t *testing.T) {
	testPipeline(t, 10*time.Millisecond, 10)
}

func TestPipeline10WorkersZeroInputPeriod(t *testing.T) {
	testPipeline(t, 0, 10)
}

func TestPipeline100WorkersLongInputPeriod(t *testing.T) {
	testPipeline(t, 20*time.Millisecond, 100)
}

func TestPipeline100WorkersMediumInputPeriod(t *testing.T) {
	testPipeline(t, 10*time.Millisecond, 100)
}

func TestPipeline100WorkersZeroInputPeriod(t *testing.T) {
	testPipeline(t, 0, 100)
}

func testPipeline(t *testing.T, inputPeriod time.Duration, numOfWorkers int) {
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
		return
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
		return
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
		return
	}

	// connect p1, p2, p3 into a pipeline
	ConnectPools(p1, p2, nil)
	ConnectPools(p2, p3, nil)

	const a = 0.1
	var inputPeriodAvg time.Duration
	lastSubmitted := time.Now()
	started := time.Now()
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
			logger.Printf("[test] submitting job%d - inputPeriodAvg: %v\n", i, inputPeriodAvg)
			p1.Submit(float64(i))
			inputPeriodNow := time.Since(lastSubmitted)
			lastSubmitted = time.Now()
			inputPeriodAvg = time.Duration(a*float64(inputPeriodNow) + (1-a)*float64(inputPeriodAvg))
		}
		logger.Printf("[test] submitted %d jobs - calling p.StopAndWait()\n", i)
		t.Logf("submitted %d jobs\n", i)
		p1.StopAndWait()
	}()

	var outputPeriodAvg time.Duration
	lastReceived := time.Now()
	for result := range p3.Results {
		outputPeriod := time.Since(lastReceived)
		lastReceived = time.Now()
		outputPeriodAvg = time.Duration(a*float64(outputPeriod) + (1-a)*float64(outputPeriodAvg))
		logger.Println("[test] result:", result.Value, "outputPeriodAvg:", outputPeriodAvg)
	}

	p1WorkersAvg, p1WorkersStd := processStats(p1Stats, started.Add(30*time.Second), lastSubmitted)
	p2WorkersAvg, p2WorkersStd := processStats(p2Stats, started.Add(30*time.Second), lastSubmitted)
	p3WorkersAvg, p3WorkersStd := processStats(p3Stats, started.Add(30*time.Second), lastSubmitted)
	t.Logf("[pool=p1] workers: avg=%v std=%v\n", p1WorkersAvg, p1WorkersStd)
	t.Logf("[pool=p2] workers: avg=%v std=%v\n", p2WorkersAvg, p2WorkersStd)
	t.Logf("[pool=p3] workers: avg=%v std=%v\n", p3WorkersAvg, p3WorkersStd)

	// p1WorkersAvg should be about 1/3 of p3WorkersAvg
	if p1WorkersAvg < 0.3*p3WorkersAvg - 1 {
		t.Errorf("p1WorkersAvg < %v", 0.3*p3WorkersAvg - 1)
	}
	if p1WorkersAvg > 0.4*p3WorkersAvg + 1 {
		t.Errorf("p1WorkersAvg > %v", 0.4*p3WorkersAvg + 1)
	}
	// p2WorkersAvg should be about 2/3 of p3WorkersAvg
	if p2WorkersAvg < 0.6*p3WorkersAvg - 1 {
		t.Errorf("p2WorkersAvg < %v", 0.6*p3WorkersAvg - 1)
	}
	if p2WorkersAvg > 0.7*p3WorkersAvg + 1 {
		t.Errorf("p2WorkersAvg > %v", 0.7*p3WorkersAvg + 1)
	}
	// p3WorkersAvg should be about p3WorkersExpected
	var p3WorkersExpected float64
	if inputPeriod > 0 {
		p3WorkersExpected = float64(jobDur3 / inputPeriod)
		if p3WorkersExpected > float64(numOfWorkers) {
			p3WorkersExpected = float64(numOfWorkers)
		}
	} else {
		p3WorkersExpected = float64(numOfWorkers)
	}
	if p3WorkersAvg < 0.9*p3WorkersExpected - 1 {
		t.Errorf("p3WorkersAvg < 0.9*%v - 1 = %v", p3WorkersExpected, 0.9*p3WorkersExpected - 1)
	}
	if p3WorkersAvg > 1.1*p3WorkersExpected + 1 {
		t.Errorf("p3WorkersAvg > 1.1*%v + 1 = %v", p3WorkersExpected, 1.1*p3WorkersExpected + 1)
	}

	// fail if standard deviation is too high
	if p1WorkersStd/p1WorkersAvg > 0.1 && p1WorkersStd > 1 {
		t.Error("p1WorkersStd too high")
	}
	if p2WorkersStd/p2WorkersAvg > 0.1 && p2WorkersStd > 1 {
		t.Error("p2WorkersStd too high")
	}
	if p3WorkersStd/p3WorkersAvg > 0.05 && p3WorkersStd > 1 {
		t.Error("p3WorkersStd too high")
	}
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
	workersAvg := float64(workersSum) / nFloat
	workersStd := math.Sqrt(float64(workersSumSq)/nFloat - math.Pow(float64(workersSum)/nFloat, 2))
	return workersAvg, workersStd
}

func loggerIfDebugEnabled() *log.Logger {
	if *flagDebugLogs {
		return log.Default()
	}
	return nil
}
