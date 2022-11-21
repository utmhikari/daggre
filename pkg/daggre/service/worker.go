package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/utmhikari/daggre/pkg/daggre"
	"log"
	"sync"
	"time"
)

type job struct {
	data  *daggre.Data
	aggre *daggre.Aggregator
}

func (j *job) Process() *daggre.AggreResult {
	// time.Sleep(500 * time.Millisecond)
	ret := j.aggre.Aggregate(j.data)
	return ret
}

type Worker struct {
	maxRunningJobs uint
	timeout        time.Duration

	running bool
	ctx     context.Context
	cancel  context.CancelFunc

	wgStart sync.WaitGroup
	wgStop  sync.WaitGroup

	jobID       uint
	resultCache map[uint]chan *daggre.AggreResult
	mu          sync.Mutex
}

func (w *Worker) IsRunning() bool {
	return w.running
}

func (w *Worker) onStop() {
	w.mu.Lock()
	defer w.mu.Unlock()

	// clear resultCache
	for _, resultChan := range w.resultCache {
		close(resultChan)
	}
	w.resultCache = make(map[uint]chan *daggre.AggreResult)
}

func (w *Worker) Start() {
	if w.IsRunning() {
		return
	}
	w.ctx, w.cancel = context.WithCancel(context.Background())
	w.wgStart.Add(1)
	w.wgStop.Add(1)
	go func() {
		w.running = true
		w.wgStart.Done()
		select {
		case <-w.ctx.Done():
			w.onStop()
			w.wgStop.Done()
			w.running = false
			return
		}
	}()
	w.wgStart.Wait()
}

func (w *Worker) Stop() {
	if !w.IsRunning() {
		return
	}
	fmt.Printf("worker will stop immediately...")
	w.cancel()
	w.wgStop.Wait()
}

func (w *Worker) setup() (jobID uint, resultChan chan *daggre.AggreResult, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	numRunningJobs := uint(len(w.resultCache))
	log.Printf("current num jobs: %d", numRunningJobs)

	if numRunningJobs >= w.maxRunningJobs {
		err = errors.New("max jobs exceeded")
		return
	}

	resultChan = make(chan *daggre.AggreResult, 1)
	w.jobID += 1
	jobID = w.jobID
	w.resultCache[jobID] = resultChan
	return
}

func (w *Worker) cache(jobID uint, ret *daggre.AggreResult) {
	w.mu.Lock()
	defer w.mu.Unlock()
	// timeout or worker stopped if key not exists
	resultChan, ok := w.resultCache[jobID]
	if ok {
		log.Printf("CACHE -> job: %d, ret: %+v", jobID, ret)
		resultChan <- ret
	}
}

func (w *Worker) uncache(jobID uint) {
	w.mu.Lock()
	defer w.mu.Unlock()
	resultChan, ok := w.resultCache[jobID]
	if ok {
		close(resultChan)
		delete(w.resultCache, jobID)
	}
}

func (w *Worker) await(ctx context.Context, resultChan chan *daggre.AggreResult) (*daggre.AggreResult, error) {
	select {
	case <-ctx.Done(): // timeout
		log.Printf("timeout!!!")
		return nil, errors.New("timeout")
	case ret := <-resultChan: // got result
		if ret == nil { // only when Stop() is called, all resultChans are closed
			return nil, errors.New("worker stopped")
		}
		return ret, nil
	}
}

func (w *Worker) process(j *job) (*daggre.AggreResult, error) {
	jobID, resultChan, err := w.setup()
	if err != nil {
		return nil, err
	}

	// process job with timeout context
	ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		ret := j.Process()
		w.cache(jobID, ret)
	}()
	wg.Wait()

	defer w.uncache(jobID)
	ret, err := w.await(ctx, resultChan)
	log.Printf("AWAIT -> job: %d, ret: %+v", jobID, ret)
	return ret, err
}

func (w *Worker) Aggregate(data *daggre.Data, aggre *daggre.Aggregator) (*daggre.AggreResult, error) {
	if data == nil {
		return nil, errors.New("data is nil")
	}
	if aggre == nil {
		return nil, errors.New("aggre is nil")
	}
	if !w.IsRunning() {
		return nil, errors.New("worker is not running")
	}
	j := &job{
		data:  data,
		aggre: aggre,
	}
	ret, err := w.process(j)
	if err != nil {
		return nil, fmt.Errorf("process error, %s", err.Error())
	}
	return ret, nil
}

func NewWorker(maxRunningJobs uint, timeout time.Duration) (*Worker, error) {
	if maxRunningJobs == 0 {
		return nil, errors.New("maxRunningJobs must be larger than 0")
	}
	if timeout == 0 {
		return nil, errors.New("timeout must be non-zero")
	}
	w := &Worker{
		maxRunningJobs: maxRunningJobs,
		timeout:        timeout,
		running:        false,
		ctx:            nil,
		cancel:         nil,
		wgStart:        sync.WaitGroup{},
		wgStop:         sync.WaitGroup{},
		jobID:          0,
		resultCache:    make(map[uint]chan *daggre.AggreResult),
		mu:             sync.Mutex{},
	}
	return w, nil
}
