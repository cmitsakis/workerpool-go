// Copyright (C) 2022 Charalampos Mitsakis (go.mitsakis.org/workerpool)
// Licensed under the Apache License, Version 2.0

package workerpool

import (
	"errors"
)

type behavior interface {
	Retryable() bool
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

// ErrorWrapRetryable marks an error as retryable.
func ErrorWrapRetryable(err error) error {
	if err == nil {
		return nil
	}
	return &retryable{Err: err}
}
