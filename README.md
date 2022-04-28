# workerpool

Generic worker pool with limited concurrency, backpressure, dynamically resizable number of workers, and composability of pools into pipelines.

**backpressure:**
The pool includes a queue with limited capacity.
If too many jobs are submitted and the queue is full, submissions block.

**auto-scaling:**
If too many jobs are in queue, new workers are started (if available).
If there are more active workers than needed some workers are stopped.

**steady-state behavior:**
If the rate of job submissions is constant, the number of active workers will quickly become almost constant, and the output rate will be equal to the input (submission) rate.

Notable differences from other worker pool libraries:

- Each worker can maintain a "connection" for the duration of the time it is active.
  A "connection" can in fact be any type of value returned by the worker initialization function. Connection is just the most obvious use-case for such value.
  This is useful if you want to implement a crawler or email sender and want to avoid reconnecting for each job.
- You don't submit a closure for each job. Instead you pass a handler function at the creation of the pool and then you submit job payloads.
- You can connect worker pools into a pipeline. That way you can increase performance by separating IO-intensive from CPU-intensive tasks.

Under development. API is subject to change.

## Installation

Requires Go 1.18

```sh
go get go.mitsakis.org/workerpool
```

## Documentation

<https://pkg.go.dev/go.mitsakis.org/workerpool>

## Usage

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
			return nil, fmt.Errorf("client.Get failed: %w", err)
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
	}, workerpool.Retries(2))

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

Bug reports, bug fixes, and ideas to improve the API, are welcome.

## License

Copyright (C) 2022 Charalampos Mitsakis (go.mitsakis.org/workerpool)

This software is licensed under the terms of the [Apache License, Version 2.0](LICENSE)
