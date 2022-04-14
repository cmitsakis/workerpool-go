// Copyright (C) 2022 Charalampos Mitsakis (go.mitsakis.org/workerpool)
// Licensed under the Apache License, Version 2.0

package workerpool

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"
)

func TestExample(t *testing.T) {
	results := make(chan float64)
	p, err := NewPoolSimple(4, func(job Job[float64], workerID int) error {
		results <- job.Payload * job.Payload
		return nil
	}, LoggerInfo(log.Default()), LoggerDebug(log.Default()))
	if err != nil {
		log.Printf("NewPoolSimple() failed: %s", err)
		return
	}
	go func() {
		for result := range results {
			log.Println("result:", result)
		}
	}()
	for i := 0; i < 100; i++ {
		log.Printf("[test] submitting job%d\n", i)
		p.Submit(float64(i))
	}
	log.Println("[test] submitted jobs - calling p.StopAndWait()")
	p.StopAndWait()
}

func TestPoolSimple(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	results := make(chan int)
	p, err := NewPoolSimple(2, func(job Job[int], workerID int) error {
		if rand.Float32() > 0.95 {
			return ErrorWrapRetryable(fmt.Errorf("job failure"))
		}
		results <- 2 * job.Payload
		return nil
	}, Retries(4), IdleTimeout(1*time.Minute), LoggerInfo(log.Default()), LoggerDebug(log.Default()))
	if err != nil {
		log.Printf("NewPoolSimple() failed: %s", err)
	}
	go func() {
		const a = 0.1
		var outputPeriodAvg time.Duration
		lastReceived := time.Now()
		for range results {
			outputPeriod := time.Since(lastReceived)
			lastReceived = time.Now()
			outputPeriodAvg = time.Duration(a*float64(outputPeriod) + (1-a)*float64(outputPeriodAvg))
			log.Println("[test] outputPeriodAvg:", outputPeriodAvg)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
loop:
	for i := 0; i < 100000; i++ {
		select {
		case <-ctx.Done():
			break loop
		default:
		}
		log.Printf("[test] submitting job%d\n", i)
		p.Submit(i)
	}
	log.Println("[test] submitted jobs - calling p.StopAndWait()")
	p.StopAndWait()
	log.Println("[test] p.StopAndWait() returned")
}

func TestPoolFull(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	workerProfiles := make([]string, 0)
	for i := 0; i < 100; i++ {
		workerProfiles = append(workerProfiles, fmt.Sprintf("w%d", i))
	}
	inputPeriod := 10 * time.Millisecond
	jobDur := 500 * time.Millisecond
	results := make(chan struct{})
	p, err := NewPoolWithInit(len(workerProfiles), func(job Job[int], workerID int, connection struct{}) error {
		worker := workerProfiles[workerID]
		log.Printf("[test/worker%v] job%d started - attempt %d - worker %v\n", workerID, job.ID, job.Attempt, worker)
		time.Sleep(jobDur)
		if rand.Float32() > 0.95 {
			return ErrorWrapRetryable(fmt.Errorf("job failure"))
		}
		results <- struct{}{}
		return nil
	}, func(workerID int) (struct{}, error) {
		time.Sleep(3 * jobDur)
		log.Printf("[test/worker%v] connecting\n", workerID)
		return struct{}{}, nil
	}, func(workerID int, connection struct{}) error {
		time.Sleep(3 * jobDur)
		log.Printf("[test/worker%v] disconnecting\n", workerID)
		return nil
	}, Retries(4), LoggerInfo(log.Default()), LoggerDebug(log.Default()))
	if err != nil {
		log.Printf("NewPool() failed: %s", err)
	}
	go func() {
		const a = 0.1
		var outputPeriodAvg time.Duration
		lastReceived := time.Now()
		for range results {
			outputPeriod := time.Since(lastReceived)
			lastReceived = time.Now()
			outputPeriodAvg = time.Duration(a*float64(outputPeriod) + (1-a)*float64(outputPeriodAvg))
			log.Println("[test] outputPeriodAvg:", outputPeriodAvg)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	for i := 0; i < 100000; i++ {
		if sleepCtx(ctx, inputPeriod) {
			break
		}
		log.Printf("[test] submitting job%d\n", i)
		p.Submit(i)
	}
	log.Println("[test] submitted jobs - calling p.StopAndWait()")
	p.StopAndWait()
	log.Println("[test] p.StopAndWait() returned")
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
