# workerpool

Generic worker pool with limited concurrency, backpressure, and dynamically resizable number of workers.

The pool includes a queue with limited capacity.
If too many jobs are submitted and the queue is full, submissions block.

If too many jobs are in queue, new workers are started (if available).
If there are more active workers than needed some workers are stopped.

Notable differences from other worker pool libraries:

- Each worker can maintain a connection. This is useful if you want to implement a crawler or email sender and want to avoid reconnecting for each job.
- You don't submit a closure for each job. Instead you pass a handler function at the creation of the pool and then you submit job payloads.

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
	results := make(chan float64)
	p, err := workerpool.NewPoolSimple(4, func(job workerpool.Job[float64], workerID int) error {
		results <- math.Sqrt(job.Payload)
		return nil
	})
	if err != nil {
		fmt.Printf("NewPoolSimple() failed: %s", err)
		return
	}
	go func() {
		for i := 0; i < 100; i++ {
			p.Submit(float64(i))
		}
		p.StopAndWait()
		close(results)
	}()
	for result := range results {
		fmt.Println("result:", result)
	}
}
```

## License

Copyright (C) 2022 Charalampos Mitsakis (go.mitsakis.org/workerpool)

This software is licensed under the terms of the [Apache License, Version 2.0](LICENSE)
