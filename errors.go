// Copyright (C) 2022 Charalampos Mitsakis (go.mitsakis.org/workerpool)
// Licensed under the Apache License, Version 2.0

package workerpool

import (
	"errors"
)

type behavior interface {
	Retryable() bool
	Unaccounted() bool
}

// errorIsRetryable returns the retryability of an error.
func errorIsRetryable(err error) bool {
	var errBehavior behavior
	if errors.As(err, &errBehavior) {
		return errBehavior.Retryable()
	}
	return false
}

type retryable struct {
	Err error
}

func (err retryable) Error() string {
	return err.Err.Error()
}

func (err retryable) Unwrap() error {
	return err.Err
}

func (err retryable) Retryable() bool {
	return true
}

func (err retryable) Unaccounted() bool {
	return false
}

// ErrorWrapRetryable marks an error as retryable.
func ErrorWrapRetryable(err error) error {
	if err == nil {
		return nil
	}
	return &retryable{Err: err}
}

func errorIsUnaccounted(err error) bool {
	var errBehavior behavior
	if errors.As(err, &errBehavior) {
		return errBehavior.Unaccounted()
	}
	return false
}

type retryableUnaccounted struct {
	Err error
}

func (err retryableUnaccounted) Error() string {
	return err.Err.Error()
}

func (err retryableUnaccounted) Unwrap() error {
	return err.Err
}

func (err retryableUnaccounted) Retryable() bool {
	return true
}

func (err retryableUnaccounted) Unaccounted() bool {
	return true
}

// ErrorWrapRetryableUnaccounted marks an error as retryable and unaccounted.
// This means the pool will keep retrying the job indefinitely,
// without incrementing the attempt counter of the job.
func ErrorWrapRetryableUnaccounted(err error) error {
	if err == nil {
		return nil
	}
	return &retryableUnaccounted{Err: err}
}
