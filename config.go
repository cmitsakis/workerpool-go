// Copyright (C) 2022 Charalampos Mitsakis (go.mitsakis.org/workerpool)
// Licensed under the Apache License, Version 2.0

package workerpool

import (
	"fmt"
	"log"
	"time"
)

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
