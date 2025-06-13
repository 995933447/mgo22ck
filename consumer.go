package main

import "github.com/995933447/log-go/v2/loggo"

func DoSync(watcherName string) error {
	watcherMu.RLock()
	watcher, ok := watchers[watcherName]
	if !ok {
		watcherMu.RUnlock()
		return nil
	}
	streamBatchLen := len(watcher.StreamList)
	ckConn := *watcher.CkConn
	watcherMu.RUnlock()

	if streamBatchLen == 0 {
		return nil
	}

	streamBatch := watcher.StreamList[0:streamBatchLen]
	err := SyncBatch2Ck(ckConn, streamBatch)

	watcherMu.Lock()
	watcher.StreamList = watcher.StreamList[streamBatchLen:]
	watcherMu.Unlock()

	if err != nil {
		// 队列的FailStreamList不需要考虑并发读写问题,队列配置限制了一个watcher的消费者同时只存在一个
		watcher.FailStreamList = append(watcher.FailStreamList, streamBatch...)
		return err
	}

	// 没有失败队列
	if len(watcher.FailStreamList) == 0 {
		err = RecResumeToken(watcherName, streamBatch[streamBatchLen-1].ResumeToken)
		if err != nil {
			loggo.Errorf("RecResumeToken err: %v", err)
		}
	}

	return nil
}

func RetrySyncFailed(watcherName string) error {
	watcherMu.RLock()
	watcher, ok := watchers[watcherName]
	if !ok {
		watcherMu.RUnlock()
		return nil
	}
	streamBatchLen := len(watcher.FailStreamList)
	ckConn := *watcher.CkConn
	watcherMu.RUnlock()

	if streamBatchLen == 0 {
		return nil
	}

	streamBatch := watcher.FailStreamList[0:streamBatchLen]
	err := SyncBatch2Ck(ckConn, streamBatch)
	if err != nil {
		return err
	}

	watcher.FailStreamList = watcher.FailStreamList[streamBatchLen:]
	if err = RecResumeToken(watcherName, streamBatch[streamBatchLen-1].ResumeToken); err != nil {
		loggo.Errorf("RecResumeToken err: %v", err)
	}

	return nil
}
