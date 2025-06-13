package main

import (
	"context"
	"github.com/995933447/log-go/v2/loggo"
	"github.com/995933447/runtimeutil"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strings"
	"time"
)

type MgoWatcher struct {
	Name                 string
	Db                   string
	ChangeStreamPipeline mongo.Pipeline
	MgoConn              *mongo.Client
	CkConn               *driver.Conn
	SyncSinceSecAgo      int64
	StreamList           []*ChangeStream
	FailStreamList       []*ChangeStream
}

type ChangeStream struct {
	Id                *WatchId               `bson:"_id"`
	Ns                NS                     `bson:"ns"`
	OperationType     string                 `bson:"operationType"`
	DocumentKey       map[string]interface{} `bson:"documentKey"`
	FullDocument      map[string]interface{} `bson:"fullDocument"`
	UpdateDescription map[string]interface{} `bson:"updateDescription"`
	ClusterTime       primitive.Timestamp    `bson:"clusterTime"`
	ResumeToken       bson.Raw
}

type WatchId struct {
	Data string `bson:"_data"`
}

type NS struct {
	Database   string `bson:"db"`
	Collection string `bson:"coll"`
}

func (w *MgoWatcher) watch() error {
	timestamp := &primitive.Timestamp{
		T: uint32(time.Now().Unix() - w.SyncSinceSecAgo),
		I: 0,
	}
	opt := options.ChangeStream().SetFullDocument(options.UpdateLookup).SetStartAtOperationTime(timestamp)

	resumeToken, err := GetResumeToken(w.Name)
	if err != nil {
		loggo.Errorf("GetResumeToken failed: %v", err)
		return err
	}

	if len(resumeToken) > 0 {
		opt.SetStartAfter(resumeToken)
		opt.SetStartAtOperationTime(nil)
	}

	changeStream, err := w.MgoConn.Watch(context.TODO(), w.ChangeStreamPipeline, opt)
	if err != nil {
		if strings.Contains(err.Error(), "ChangeStreamHistoryLost") || strings.Contains(err.Error(), "resume token was not found") {
			// ResumeToken过期
			err = RecWatcherErr(w.Name, err)
			if err != nil {
				loggo.Errorf("RecWatcherErr %v", err)
				return err
			}

			opt = options.ChangeStream().SetFullDocument(options.UpdateLookup).SetStartAtOperationTime(timestamp)
			changeStream, err = w.MgoConn.Watch(context.TODO(), w.ChangeStreamPipeline, opt)
			if err != nil {
				loggo.Errorf("watch change stream err:%v", err)
				return err
			}
		} else if strings.Contains(err.Error(), "BSONObjectTooLarge") {
			err = RecWatcherErr(w.Name, err)
			if err != nil {
				loggo.Errorf("RecWatcherErr %v", err)
				return err
			}

			time.Sleep(time.Second)

			opt.SetStartAfter(nil)
			opt.SetStartAtOperationTime(&primitive.Timestamp{T: uint32(time.Now().Unix()), I: 0})
			changeStream, err = w.MgoConn.Watch(context.TODO(), w.ChangeStreamPipeline, opt)
			if err != nil {
				loggo.Errorf("watch change stream err:%v", err)
				return err
			}
		} else {
			return err
		}
	}

	runtimeutil.Go(func() {
		for changeStream.Next(context.TODO()) {
			if paused.Load() {
				break
			}

			var streamObj ChangeStream
			if err = changeStream.Decode(&streamObj); err != nil {
				continue
			}

			streamObj.ResumeToken = changeStream.ResumeToken()

			if strings.Contains(streamObj.Ns.Collection, ".") ||
				strings.Contains(streamObj.Ns.Collection, "-") ||
				strings.Contains(streamObj.Ns.Collection, "mongosync") {
				continue
			}

			watcherMu.Lock()
			w.StreamList = append(w.StreamList, &streamObj)
			watcherMu.Unlock()

			for {
				if err = PushQueue(w); err == nil {
					break
				}
				time.Sleep(1 * time.Second)
			}

			for {
				watcherMu.RLock()
				sizeOfWatchers := EstimateSize(watchers)
				watcherMu.RUnlock()

				// 大于1G先不读新的数据，等消费完
				if sizeOfWatchers < 1024*1024*1024 {
					break
				}

				time.Sleep(time.Second)
			}
		}

		if err = changeStream.Err(); err != nil {
			if strings.Contains(err.Error(), "ChangeStreamHistoryLost") ||
				strings.Contains(err.Error(), "resume token was not found") {
				err = RecWatcherErr(w.Name, err)
				if err != nil {
					loggo.Errorf("record watcher err occur exception:%v", err)
				}

				if err = w.watch(); err != nil {
					loggo.Errorf("watch err:%v", err)
				}
			}
		}
	})

	return nil
}
