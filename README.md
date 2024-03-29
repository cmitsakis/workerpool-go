# workerpool

Worker pool library with auto-scaling, backpressure, and easy composability of pools into pipelines. Uses Go 1.18 generics.

**Notable differences from other worker pool libraries:**

- Each worker runs init/deinit functions (if set) when it starts/stops respectively,
  and stores the value returned by the init function (e.g. a connection, or external process) for the duration of the time it is active.
  This way you can easily create a **connection pool** for a crawler or email sender.
- You don't submit a function for each job. Instead you pass a handler function at the creation of the pool and then you submit a value (payload) for each job.
- You can connect worker pools into a **pipeline**. This way you can increase performance by separating IO-intensive from CPU-intensive tasks (see [crawler example](#crawler-pipeline-example)), or IO tasks of different parallelizability (e.g. crawling and saving to disk).

**backpressure:**
The pool includes a queue with limited capacity.
If the queue is full, job submissions block until they can be put in queue.

**auto-scaling:**
If too many jobs are in queue, new workers are started (if available).
If there are more active workers than needed, some workers (and their respective goroutines) are stopped.
You can disable auto-scaling for CPU intensive tasks.

**steady-state behavior:**
If the rate of job submissions is constant, the number of active workers will quickly become almost constant, and the output rate will be equal to the input (submission) rate.

## Installation

Requires Go 1.18+

```sh
go get go.mitsakis.org/workerpool
```

## API Reference

<https://pkg.go.dev/go.mitsakis.org/workerpool>

## Usage

### Type parameters

Type `Pool[I, O, C any]` uses three type parameters:

- `I`: input (job payload) type
- `O`: output (result) type
- `C`: type returned by the `workerInit()` function (e.g. a connection)

### Constructors

You might not need all three type parameters so for convenience you can create a pool by using a constructor that hides some type parameters.
That's why there are four constructors of increasing complexity:

```go
NewPoolSimple(
	numOfWorkers int,
	handler func(job Job[I], workerID int) error,
	...)

NewPoolWithInit(
	numOfWorkers int,
	handler func(job Job[I], workerID int, connection C) error,
	workerInit func(workerID int) (C, error),
	workerDeinit func(workerID int, connection C) error,
	...)

NewPoolWithResults(
	numOfWorkers int,
	handler func(job Job[I], workerID int) (O, error),
	...)

NewPoolWithResultsAndInit(
	numOfWorkers int,
	handler func(job Job[I], workerID int, connection C) (O, error),
	workerInit func(workerID int) (C, error),
	workerDeinit func(workerID int, connection C) error,
	...)
```

### Simple pool

In the most simple case you create a pool (`p`), submit the job payloads, and then call `p.StopAndWait()` to signal that you are done submitting jobs.
`StopAndWait()` blocks until all submitted jobs have run and workers have stopped.
```go
p, _ := workerpool.NewPoolSimple(...)
for i := 0; i < 100; i++ {
	p.Submit(i)
}
p.StopAndWait()
```

### Pool with results

If you create a pool with results (using the constructors `NewPoolWithResults()` or `NewPoolWithResultsAndInit()`), the pool writes each job's result to the `p.Results` channel.

**Warning:** You *must* read from `p.Results`. Submissions and reading from `p.Results` channel *must* happen concurrently (from different goroutines) otherwise there will be a deadlock.
```go
p, _ := workerpool.NewPoolWithResults(10, func(job workerpool.Job[float64], workerID int) (float64, error) {
	return math.Sqrt(job.Payload), nil
})
go func() {
	for i := 0; i < 100; i++ {
		p.Submit(float64(i))
	}
	p.StopAndWait()
}()
for result := range p.Results {
}
```

### Pool with worker init/deinit

You can create a pool using the constructors `NewPoolWithInit()` or `NewPoolWithResultsAndInit()`, if you want each worker to:
- run an `init()` function when it starts, and store the value returned by `init()`
- run a `deinit()` when it stops

This is useful, if you need each worker to have a connection or an external process.
```go
p, _ := workerpool.NewPoolWithInit(5, func(job workerpool.Job[string], workerID int, tr *http.Transport) error {
    // handler function
}, func(workerID int) (*http.Transport, error) { // init
    return &http.Transport{}, nil
}, func(workerID int, tr *http.Transport) error { // deinit
    tr.CloseIdleConnections()
})
```

Notice that the `http.Transport` created by the init function is accessible in the handler function as a parameter.

### Pipeline

You can also connect pools of compatible type (results of `p1` are the same type as inputs to `p2`) into a pipeline by using the `ConnectPools(p1, p2, handleError)` function like this:
```go
workerpool.ConnectPools(p1, p2, func(result workerpool.Result[string, []byte]) {
	// log error
})
```
By connecting two pools, results of `p1` that have no error are submitted to `p2`, and those with an error are handled by the `handleError()` function.

### Retrying failed jobs

The pool will retry failed jobs, if in the handler function you wrap the error with `ErrorWrapRetryable()` like this:
```go
p, _ := workerpool.NewPoolSimple(4, func(job workerpool.Job[float64], workerID int) error {
	...
	if err != nil {
		return workerpool.ErrorWrapRetryable(fmt.Errorf("error: %w", err))
	}
	return nil
})
```

By default a failed job will be retried only once.
You can increase the number of retries by creating a pool with `Retries()` like this:
```go
p, _ := workerpool.NewPoolSimple(4, func(job workerpool.Job[float64], workerID int) error {
	...
}, workerpool.Retries(3))
```

### Examples

#### Simple example
```go
package main

import (
	"fmt"
	"math"

	"go.mitsakis.org/workerpool"
)

func main() {
	p, _ := workerpool.NewPoolSimple(4, func(job workerpool.Job[float64], workerID int) error {
		result := math.Sqrt(job.Payload)
		fmt.Println("result:", result)
		return nil
	})
	for i := 0; i < 100; i++ {
		p.Submit(float64(i))
	}
	p.StopAndWait()
}
```

#### Pipeline example

A more complicated example with three pools connected into a pipeline.
```go
package main

import (
	"fmt"
	"math"

	"go.mitsakis.org/workerpool"
)

func main() {
	// stage 1: calculate square root
	p1, _ := workerpool.NewPoolWithResults(10, func(job workerpool.Job[float64], workerID int) (float64, error) {
		return math.Sqrt(job.Payload), nil
	})

	// stage 2: negate number
	p2, _ := workerpool.NewPoolWithResults(10, func(job workerpool.Job[float64], workerID int) (float64, error) {
		return -job.Payload, nil
	})

	// stage 3: convert float to string
	p3, _ := workerpool.NewPoolWithResults(10, func(job workerpool.Job[float64], workerID int) (string, error) {
		return fmt.Sprintf("%.3f", job.Payload), nil
	})

	// connect p1, p2, p3 into a pipeline
	workerpool.ConnectPools(p1, p2, nil)
	workerpool.ConnectPools(p2, p3, nil)

	go func() {
		for i := 0; i < 100; i++ {
			p1.Submit(float64(i))
		}
		p1.StopAndWait()
	}()

	for result := range p3.Results {
		fmt.Println("result:", result.Value)
	}
}
```

#### Crawler pipeline example

A real world example with two pools.
The first pool (p1) downloads URLs and the second (p2) processes the downloaded documents.
Each worker has its own http.Transport that is reused between requests.
```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"go.mitsakis.org/workerpool"
)

func main() {
	// pool p1 downloads URLs
	p1, _ := workerpool.NewPoolWithResultsAndInit(5, func(job workerpool.Job[string], workerID int, tr *http.Transport) ([]byte, error) {
		client := &http.Client{
			Transport: tr,
			Timeout:   30 * time.Second,
		}
		resp, err := client.Get(job.Payload)
		if err != nil {
			// mark error as retryable
			return nil, workerpool.ErrorWrapRetryable(fmt.Errorf("client.Get failed: %w", err))
		}
		if resp.StatusCode < 200 || resp.StatusCode > 399 {
			return nil, fmt.Errorf("HTTP status code %d", resp.StatusCode)
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read response body: %w", err)
		}
		return body, nil
	}, func(workerID int) (*http.Transport, error) { // worker init function
		return &http.Transport{}, nil
	}, func(workerID int, tr *http.Transport) error { // worker deinit function
		tr.CloseIdleConnections()
		return nil
	}, workerpool.Retries(2)) // retry twice if error is retryable

	// pool p2 processes the content of the URLs downloaded by p1
	p2, _ := workerpool.NewPoolWithResults(1, func(job workerpool.Job[[]byte], workerID int) (int, error) {
		numOfLines := bytes.Count(job.Payload, []byte("\n"))
		return numOfLines, nil
	}, workerpool.FixedWorkers()) // we use a fixed number of workers (1) because it's a CPU intensive task

	// connect pools p1, p2 into a pipeline.
	// documents downloaded by p1 are submitted to p2 for further processing.
	workerpool.ConnectPools(p1, p2, func(result workerpool.Result[string, []byte]) {
		if result.Error != nil {
			log.Printf("failed to download URL %v - error: %v", result.Job.Payload, result.Error)
		}
	})

	go func() {
		urls := []string{
			"http://example.com/",
			// add your own URLs
		}
		for _, u := range urls {
			time.Sleep(100 * time.Millisecond)
			log.Println("submitting URL:", u)
			p1.Submit(u)
		}
		p1.StopAndWait()
	}()

	for result := range p2.Results {
		log.Printf("web page has %d lines\n", result.Value)
	}
}
```

## Contributing

### Non-code Contributions

Bug reports, and ideas to improve the API or the auto-scaling behavior, are welcome.

### Code Contributions

Bug fixes, and improvements to auto-scaling (implementation or tests), are welcome.

Correctness tests (`go test -run Correctness`) must pass, and auto-scaling behavior tests (`go test -run Autoscaling -v -timeout 30m`) should not become worse.

## License

Copyright (C) 2022 Charalampos Mitsakis (go.mitsakis.org/workerpool)

This software is licensed under the terms of the [Apache License, Version 2.0](LICENSE)
