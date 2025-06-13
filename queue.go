package main

import (
	"github.com/995933447/bucketsched"
	"sync"
)

var queue *bucketsched.BucketQueue

var (
	queueMu             sync.RWMutex
	inQueueWatchers     = map[string]struct{}{}
	inFailQueueWatchers = map[string]struct{}{}
)

func InitQueue() {
	queue = bucketsched.NewBucketQueueWithBucketConcur("mgo2ck", 10, 1)
	go queue.Run()
}

func MustQueue() *bucketsched.BucketQueue {
	if queue == nil {
		panic("MustQueue(): queue is nil")
	}

	return queue
}

func PushQueue(watcher *MgoWatcher) error {
	queueMu.RLock()
	if _, ok := inQueueWatchers[watcher.Name]; ok {
		queueMu.RUnlock()
		return nil
	}
	queueMu.RUnlock()

	queueMu.Lock()
	defer queueMu.Unlock()

	inQueueWatchers[watcher.Name] = struct{}{}
	bucketId, _ := FnvHash(watcher.Name)
	return MustQueue().Reg(bucketsched.NewTask(int64(bucketId), watcher.Name, func(task *bucketsched.Task) error {
		queueMu.Lock()
		delete(inQueueWatchers, watcher.Name)
		queueMu.Unlock()

		if err := DoSync(watcher.Name); err != nil {
			_ = PushFailQueue(watcher)
			return err
		}

		return nil
	}, 0, 1))
}

func PushFailQueue(watcher *MgoWatcher) error {
	queueMu.RLock()
	if _, ok := inFailQueueWatchers[watcher.Name]; ok {
		queueMu.RUnlock()
		return nil
	}
	queueMu.RUnlock()

	queueMu.Lock()
	defer queueMu.Unlock()

	inFailQueueWatchers[watcher.Name] = struct{}{}
	bucketId, _ := FnvHash(watcher.Name)
	return MustQueue().Reg(bucketsched.NewTask(int64(bucketId), watcher.Name, func(task *bucketsched.Task) error {
		queueMu.Lock()
		delete(inFailQueueWatchers, watcher.Name)
		queueMu.Unlock()

		if err := RetrySyncFailed(watcher.Name); err != nil {
			_ = PushFailQueue(watcher)
			return err
		}

		return nil
	}, 10, 1))
}
