package worker_pool

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/gammazero/deque"
)

const (
	idleTimeout = 2 * time.Second
)

type WorkerPool struct {
	maxWorkers   int
	taskQueue    chan func()
	workerQueue  chan func()
	stoppedChan  chan struct{}
	stopSignal   chan struct{}
	waitingQueue deque.Deque[func()]
	stopLock     sync.Mutex
	stopOnce     sync.Once
	stopped      bool
	waiting      int32
	wait         bool
}

func NewWorkerPool(numberOfWorkers int) *WorkerPool {
	if numberOfWorkers < 1 {
		numberOfWorkers = 1
	}

	pool := &WorkerPool{
		maxWorkers:  numberOfWorkers,
		taskQueue:   make(chan func()),
		workerQueue: make(chan func()),
		stopSignal:  make(chan struct{}),
		stoppedChan: make(chan struct{}),
	}

	go pool.dispatch()

	return pool
}

func (wp *WorkerPool) Submit(task func()) {
	if task != nil {
		wp.stopLock.Lock()
		stopped := wp.stopped
		wp.stopLock.Unlock()
		if stopped {
			panic("submit to stopped pool")
		}
		wp.taskQueue <- task
	}
}

func (wp *WorkerPool) SubmitWait(task func()) {
	if task == nil {
		return
	}
	doneChan := make(chan func())
	wp.stopLock.Lock()
	stopped := wp.stopped
	wp.stopLock.Unlock()
	if stopped {
		panic("submit to stopped pool")
	}
	wp.taskQueue <- func() {
		task()
		close(doneChan)
	}
	<-doneChan
}

func (wp *WorkerPool) Stop() {
	wp.stop(false)
}

func (wp *WorkerPool) StopWait() {
	wp.stop(true)
}

func (wp *WorkerPool) dispatch() {
	defer close(wp.stoppedChan)
	timeout := time.NewTimer(idleTimeout)
	var workerCount int
	var idle bool
	var wg sync.WaitGroup

Loop:
	for {
		if wp.waitingQueue.Len() != 0 {
			if !wp.processWaitingQueue() {
				break Loop
			}
			continue
		}

		select {
		case task, ok := <-wp.taskQueue:
			if !ok {
				break Loop
			}
			select {
			case wp.workerQueue <- task:
			default:
				if workerCount < wp.maxWorkers {
					wg.Add(1)
					go worker(task, wp.workerQueue, &wg)
					workerCount++
				} else {
					wp.waitingQueue.PushBack(task)
					atomic.StoreInt32(&wp.waiting, int32(wp.waitingQueue.Len()))
				}
			}
			idle = false
		case <-timeout.C:
			if idle && workerCount > 0 {
				if wp.killIdleWorker() {
					workerCount--
				}
			}
			idle = true
			timeout.Reset(idleTimeout)
		}
	}

	if wp.wait {
		wp.runQueuedTasks()
	}

	for workerCount > 0 {
		wp.workerQueue <- nil
		workerCount--
	}
	wg.Wait()

	timeout.Stop()
}

func (wp *WorkerPool) processWaitingQueue() bool {
	select {
	case task, ok := <-wp.taskQueue:
		if !ok {
			return false
		}
		wp.waitingQueue.PushBack(task)
	case wp.workerQueue <- wp.waitingQueue.Front():
		wp.waitingQueue.PopFront()
	}
	atomic.StoreInt32(&wp.waiting, int32(wp.waitingQueue.Len()))
	return true
}

func (wp *WorkerPool) runQueuedTasks() {
	for wp.waitingQueue.Len() != 0 {
		wp.workerQueue <- wp.waitingQueue.PopFront()
		atomic.StoreInt32(&wp.waiting, int32(wp.waitingQueue.Len()))
	}
}

func (wp *WorkerPool) killIdleWorker() bool {
	select {
	case wp.workerQueue <- nil:
		// Sent kill signal to worker.
		return true
	default:
		return false
	}
}

func worker(task func(), workerQueue chan func(), wg *sync.WaitGroup) {
	for task != nil {
		task()
		task = <-workerQueue
	}
	wg.Done()
}

func (wp *WorkerPool) stop(wait bool) {
	wp.stopOnce.Do(func() {
		close(wp.stopSignal)
		wp.stopLock.Lock()
		wp.stopped = true
		wp.stopLock.Unlock()
		wp.wait = wait
		close(wp.taskQueue)
	})
	<-wp.stoppedChan
}
