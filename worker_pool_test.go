package worker_pool

import (
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
)

const max = 20

func TestExample(t *testing.T) {
	defer goleak.VerifyNone(t)

	wp := NewWorkerPool(2)
	requests := []string{"alpha", "beta", "gamma", "delta", "epsilon"}

	rspChan := make(chan string, len(requests))
	for _, r := range requests {
		r := r
		wp.Submit(func() {
			rspChan <- r
		})
	}

	wp.StopWait()

	close(rspChan)
	rspSet := map[string]struct{}{}
	for rsp := range rspChan {
		rspSet[rsp] = struct{}{}
	}
	if len(rspSet) < len(requests) {
		t.Fatal("Did not handle all requests")
	}
	for _, req := range requests {
		if _, ok := rspSet[req]; !ok {
			t.Fatal("Missing expected values:", req)
		}
	}
}

func TestReuseWorkers(t *testing.T) {
	defer goleak.VerifyNone(t)

	wp := NewWorkerPool(5)
	defer wp.Stop()

	release := make(chan struct{})

	for i := 0; i < 10; i++ {
		wp.Submit(func() { <-release })
		release <- struct{}{}
		time.Sleep(time.Millisecond)
	}
	close(release)

	if countReady(wp) > 1 {
		t.Fatal("Worker not reused")
	}
}

func TestSubmitWait(t *testing.T) {
	defer goleak.VerifyNone(t)

	wp := NewWorkerPool(1)
	defer wp.Stop()

	// Check that these are noop.
	wp.Submit(nil)
	wp.SubmitWait(nil)

	done1 := make(chan struct{})
	wp.Submit(func() {
		time.Sleep(100 * time.Millisecond)
		close(done1)
	})
	select {
	case <-done1:
		t.Fatal("Submit did not return immediately")
	default:
	}

	done2 := make(chan struct{})
	wp.SubmitWait(func() {
		time.Sleep(100 * time.Millisecond)
		close(done2)
	})
	select {
	case <-done2:
	default:
		t.Fatal("SubmitWait did not wait for function to execute")
	}
}

func TestOverflow(t *testing.T) {
	defer goleak.VerifyNone(t)

	wp := NewWorkerPool(2)
	defer wp.Stop()
	releaseChan := make(chan struct{})

	for i := 0; i < 64; i++ {
		wp.Submit(func() { <-releaseChan })
	}

	go func() {
		<-time.After(time.Millisecond)
		close(releaseChan)
	}()
	wp.Stop()

	qlen := wp.waitingQueue.Len()
	if qlen != 62 {
		t.Fatal("Expected 62 tasks in waiting queue, have", qlen)
	}
}

func TestStopRace(t *testing.T) {
	defer goleak.VerifyNone(t)

	wp := NewWorkerPool(max)
	defer wp.Stop()

	workRelChan := make(chan struct{})

	var started sync.WaitGroup
	started.Add(max)

	for i := 0; i < max; i++ {
		wp.Submit(func() {
			started.Done()
			<-workRelChan
		})
	}

	started.Wait()

	const doneCallers = 5
	stopDone := make(chan struct{}, doneCallers)
	for i := 0; i < doneCallers; i++ {
		go func() {
			wp.Stop()
			stopDone <- struct{}{}
		}()
	}

	select {
	case <-stopDone:
		t.Fatal("Stop should not return in any goroutine")
	default:
	}

	close(workRelChan)

	timeout := time.After(time.Second)
	for i := 0; i < doneCallers; i++ {
		select {
		case <-stopDone:
		case <-timeout:
			wp.Stop()
			t.Fatal("timedout waiting for Stop to return")
		}
	}
}

func TestWorkerLeak(t *testing.T) {
	defer goleak.VerifyNone(t)

	const workerCount = 100

	wp := NewWorkerPool(workerCount)

	for i := 0; i < workerCount; i++ {
		wp.Submit(func() {
			time.Sleep(time.Millisecond)
		})
	}

	wp.Stop()
}

func countReady(w *WorkerPool) int {
	// Try to stop max workers.
	timeout := time.After(100 * time.Millisecond)
	release := make(chan struct{})
	wait := func() {
		<-release
	}
	var readyCount int
	for i := 0; i < max; i++ {
		select {
		case w.workerQueue <- wait:
			readyCount++
		case <-timeout:
			i = max
		}
	}

	close(release)
	return readyCount
}

/*

Run benchmarking with: go test -bench '.'

*/

func BenchmarkEnqueue(b *testing.B) {
	wp := NewWorkerPool(1)
	defer wp.Stop()
	releaseChan := make(chan struct{})

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		wp.Submit(func() { <-releaseChan })
	}
	close(releaseChan)
}

func BenchmarkEnqueue2(b *testing.B) {
	wp := NewWorkerPool(2)
	defer wp.Stop()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		releaseChan := make(chan struct{})
		for i := 0; i < 64; i++ {
			wp.Submit(func() { <-releaseChan })
		}
		close(releaseChan)
	}
}

func BenchmarkExecute1Worker(b *testing.B) {
	benchmarkExecWorkers(1, b)
}

func BenchmarkExecute2Worker(b *testing.B) {
	benchmarkExecWorkers(2, b)
}

func BenchmarkExecute4Workers(b *testing.B) {
	benchmarkExecWorkers(4, b)
}

func BenchmarkExecute16Workers(b *testing.B) {
	benchmarkExecWorkers(16, b)
}

func BenchmarkExecute64Workers(b *testing.B) {
	benchmarkExecWorkers(64, b)
}

func BenchmarkExecute1024Workers(b *testing.B) {
	benchmarkExecWorkers(1024, b)
}

func benchmarkExecWorkers(n int, b *testing.B) {
	wp := NewWorkerPool(n)
	defer wp.Stop()
	var allDone sync.WaitGroup
	allDone.Add(b.N * n)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < n; j++ {
			wp.Submit(func() {
				allDone.Done()
			})
		}
	}
	allDone.Wait()
}

func TestSubmitAfterStop_Panics(t *testing.T) {
	defer goleak.VerifyNone(t)
	wp := NewWorkerPool(1)
	wp.Stop()
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic on Submit after Stop")
		}
	}()
	wp.Submit(func() {})
}
