package main

import (
	"errors"
	"sync"
	"time"
)

type QueueMap struct {
	m  map[string]*Queue
	mu sync.RWMutex
}

func NewQueueMap() *QueueMap {
	return &QueueMap{
		m: make(map[string]*Queue),
	}
}

var DefaultQM = NewQueueMap()

type Queue struct {
	id  string
	qm  *QueueMap
	End chan int
	C   chan []string

	once sync.Once
}

func (qm *QueueMap) Add(q *Queue) {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	qm.m[q.id] = q
	q.qm = qm
}

func (qm *QueueMap) Del(q *Queue) {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	delete(qm.m, q.id)
	if q.qm == qm {
		q.qm = nil
	}
}

func (qm *QueueMap) Get(id string) *Queue {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	return qm.m[id]
}

func (qm *QueueMap) Append(val []string) {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	for _, v := range qm.m {
		v.Append(val)
	}
}

func NewQueue(id string) *Queue {
	return &Queue{
		id:  id,
		End: make(chan int, 1),
		C:   make(chan []string, 2*1024),
	}
}
func (q *Queue) Append(val []string) {
	select {
	case q.C <- val:

	case <-time.After(1 * time.Second):
		q.Close()
	}
}
func (q *Queue) Retrieve(timeout time.Duration) ([]string, error) {
	for {
		select {
		case <-q.End:
			return nil, errors.New("queue closed")
		case out := <-q.C:
			return out, nil
		}
	}
}
func (q *Queue) RetrieveTimeout(timeout time.Duration) (res [][]string, err error) {
	for {
		select {
		case <-q.End:
			err = errors.New("queue closed")
			return
		case out := <-q.C:
			res = append(res, out)
		case <-time.After(timeout):
			return
		}
	}
}

func (q *Queue) Close() {
	q.once.Do(func() {
		q.End <- 1
		close(q.End)
		if q.qm != nil {
			q.qm.Del(q)
		}
	})
}
