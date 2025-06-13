package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/995933447/log-go/v2/loggo"
	"github.com/995933447/runtimeutil"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	mgoConns  = make(map[string]*mongo.Client)
	ckConns   = make(map[string]*driver.Conn)
	watchers  = make(map[string]*MgoWatcher)
	watcherMu sync.RWMutex
	paused    atomic.Bool
)

func reload(confFileName string) error {
	paused.Store(true)

	removeStoppedWatchers()

	tk := time.NewTicker(time.Second * 5)
	defer tk.Stop()
	for len(watchers) > 0 {
		loggo.Info("some sync working, retry after 5 seconds...")
		select {
		case <-tk.C:
			removeStoppedWatchers()
		}
	}

	if err := InitConf(confFileName); err != nil {
		return err
	}

	if err := InitLog(); err != nil {
		return err
	}

	mgoConns = make(map[string]*mongo.Client)
	ckConns = make(map[string]*driver.Conn)

	if err := InitDbConns(); err != nil {
		return err
	}

	if err := InitSysRequiredMgo(); err != nil {
		return err
	}

	if err := InitWatchers(); err != nil {
		return err
	}

	paused.Store(false)

	return nil
}

func stop() {
	paused.Store(true)

	removeStoppedWatchers()

	tk := time.NewTicker(time.Second * 5)
	defer tk.Stop()
	for len(watchers) > 0 {
		select {
		case <-tk.C:
			removeStoppedWatchers()
		}
	}

	for MustQueue().Size() > 0 {
		time.Sleep(time.Second * 5)
	}
}

func removeStoppedWatchers() {
	watcherMu.Lock()
	defer watcherMu.Unlock()

	var removedWatcherNames []string
	for _, watcher := range watchers {
		// 有未消费的消息
		if len(watcher.FailStreamList) != 0 || len(watcher.StreamList) != 0 {
			continue
		}

		removedWatcherNames = append(removedWatcherNames, watcher.Name)
	}

	for _, watcherName := range removedWatcherNames {
		delete(watchers, watcherName)
	}
}

func start(confFileName string) error {
	if err := InitConf(confFileName); err != nil {
		return err
	}

	if err := InitLog(); err != nil {
		return err
	}

	if err := InitDbConns(); err != nil {
		return err
	}

	if err := InitSysRequiredMgo(); err != nil {
		return err
	}

	InitQueue()

	if err := InitWatchers(); err != nil {
		return err
	}

	return nil
}

func InitLog() error {
	sevName := MustConf().Name
	if sevName == "" {
		sevName = "mgo2ck"
	}

	loggo.SetModuleName(sevName)

	if err := loggo.InitDefaultCfgLoader("", &MustConf().Log); err != nil {
		return err
	}

	if err := loggo.InitDefaultLogger(nil); err != nil {
		return err
	}

	loggo.EnableStdoutPrinter()

	return nil
}

func watch() error {
	errGrp := runtimeutil.NewErrGrp()
	for _, watcher := range watchers {
		w := watcher
		errGrp.Go(func() error {
			if err := w.watch(); err != nil {
				loggo.Errorf("watcher %s failed: %v", w.Name, err)
				return err
			}
			return nil
		})
	}
	if err := errGrp.Wait(); err != nil {
		return err
	}

	return nil
}

func InitDbConns() error {
	cfg := MustConf()
	for _, c := range cfg.MapMgo2Ck {
		mgoConnUrl := c.MgoConn.GetConnUrl()
		_, ok := mgoConns[mgoConnUrl]
		if !ok {
			mgoConn, err := NewMgoConn(c.MgoConn)
			if err != nil {
				return err
			}

			mgoConns[mgoConnUrl] = mgoConn
		}

		ckConnName := c.CkConn.GetName()
		_, ok = ckConns[ckConnName]
		if !ok {
			ckConn, err := NewCkConn(c.CkConn)
			if err != nil {
				return err
			}

			ckConns[ckConnName] = &ckConn
		}
	}

	mgoConnUrl := cfg.SysMgoConn.GetConnUrl()
	_, ok := mgoConns[mgoConnUrl]
	if !ok {
		mgoConn, err := NewMgoConn(cfg.SysMgoConn)
		if err != nil {
			return err
		}

		mgoConns[mgoConnUrl] = mgoConn
	}

	return nil
}

func InitWatchers() error {
	for _, c := range MustConf().MapMgo2Ck {
		mgoConn, ok := mgoConns[c.MgoConn.GetConnUrl()]
		if !ok {
			return fmt.Errorf("mgoConn %s not exist", c.MgoConn.GetConnUrl())
		}

		ckConn, ok := ckConns[c.CkConn.GetName()]
		if !ok {
			return fmt.Errorf("ckConn %s not exist", c.CkConn.GetName())
		}

		skipDbSet := make(map[string]struct{})
		for _, db := range c.SkipDbs {
			skipDbSet[db] = struct{}{}
		}

		if c.AllDb {
			dbList, err := mgoConn.ListDatabaseNames(context.TODO(), bson.D{})
			if err != nil {
				return err
			}

			for _, db := range dbList {
				if _, ok := skipDbSet[db]; ok {
					continue
				}

				var skip bool
				for _, dbPrefix := range c.SkipDbPrefixes {
					if strings.Contains(db, dbPrefix) {
						skip = true
						break
					}
				}
				if skip {
					continue
				}

				watcherName := c.MgoConn.GetConnUrl() + "#" + db

				pipeline := mongo.Pipeline{
					bson.D{
						{"$match", bson.M{
							"$and": bson.A{
								bson.M{"operationType": bson.M{"$in": c.Opts}},
								bson.M{"ns.db": bson.M{"$eq": db}},
							},
						}},
					},
				}

				watcher := &MgoWatcher{
					Name:                 watcherName,
					Db:                   db,
					ChangeStreamPipeline: pipeline,
					MgoConn:              mgoConn,
					CkConn:               ckConn,
					SyncSinceSecAgo:      c.SyncSinceSecAgo,
				}

				watchers[watcherName] = watcher
			}

			continue
		}

		for _, dbConf := range c.Dbs {
			if _, ok := skipDbSet[dbConf.Db]; ok {
				continue
			}

			var skip bool
			for _, dbPrefix := range c.SkipDbPrefixes {
				if strings.Contains(dbConf.Db, dbPrefix) {
					skip = true
					break
				}
			}
			if skip {
				continue
			}

			filter := bson.A{}
			dbOpts := dbConf.Opts
			if dbConf.AllTb {
				filter = append(filter, bson.M{"$and": bson.A{
					bson.M{"operationType": bson.M{"$in": dbOpts}},
					bson.M{"ns.db": bson.M{"$eq": dbConf.Db}},
				}})
			} else {
				skipTbSet := make(map[string]struct{})
				for _, tb := range dbConf.SkipTbs {
					skipTbSet[tb] = struct{}{}
				}

				for _, tbConf := range dbConf.Tbs {
					if _, ok := skipTbSet[tbConf.Tb]; ok {
						continue
					}

					var skip bool
					for _, tbPrefix := range dbConf.SkipTbPrefixes {
						if strings.Contains(tbConf.Tb, tbPrefix) {
							skip = true
							break
						}
					}
					if skip {
						continue
					}

					tbOpts := dbOpts
					if len(tbConf.Opts) > 0 {
						tbOpts = tbConf.Opts
					}
					filter = append(filter, bson.M{"$and": bson.A{
						bson.M{"operationType": bson.M{"$in": tbOpts}},
						bson.M{"ns.db": bson.M{"$eq": dbConf.Db}},
						bson.M{"ns.coll": bson.M{"$eq": tbConf.Tb}},
					}})
				}
			}

			pipeline, err := normalizePipeline(filter)
			if err != nil {
				return err
			}

			watcherName := c.MgoConn.GetConnUrl() + "#" + dbConf.Db

			watcher := &MgoWatcher{
				Name:                 watcherName,
				Db:                   dbConf.Db,
				ChangeStreamPipeline: pipeline,
				MgoConn:              mgoConn,
				CkConn:               ckConn,
				SyncSinceSecAgo:      c.SyncSinceSecAgo,
			}

			watchers[watcherName] = watcher
		}
	}

	return watch()
}

func normalizePipeline(filter bson.A) (mongo.Pipeline, error) {
	if filter == nil || len(filter) <= 0 {
		return mongo.Pipeline{}, errors.New("no mongo ns config")
	} else if len(filter) == 1 {
		return mongo.Pipeline{bson.D{{"$match", filter[0]}}}, nil
	}
	return mongo.Pipeline{bson.D{{"$match", bson.M{"$or": filter}}}}, nil
}

func NewCkConn(cfg CkConnConf) (driver.Conn, error) {
	// 建立连接
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.Host},
		Auth: clickhouse.Auth{
			Username: cfg.User,
			Password: cfg.Pwd,
		},
		ClientInfo: clickhouse.ClientInfo{
			Products: []struct {
				Name    string
				Version string
			}{
				{Name: "mgo2ck", Version: "0.1"},
			},
		},
	})
	if err != nil {
		return nil, err
	}
	// 测试连接
	if err = conn.Ping(context.Background()); err != nil {
		return nil, err
	}
	return conn, nil
}

func NewMgoConn(cfg MgoConnConf) (*mongo.Client, error) {
	wc := writeconcern.New(writeconcern.WMajority())
	wc = writeconcern.New(writeconcern.W(1))
	opts := options.Client().ApplyURI(cfg.GetConnUrl())
	opts.SetMaxPoolSize(uint64(cfg.PoolNum))                               // 设置最大连接数
	opts.SetMinPoolSize(uint64(cfg.MinPoolNum))                            // 设置最小连接数
	opts.SetWriteConcern(wc)                                               // 写关注为1个节点确认 writeconcern.WMajority() 请求确认写操作传播到大多数mongod实例
	opts.SetReadConcern(readconcern.Majority())                            // 指定查询应返回实例的最新数据确认为，已写入副本集中的大多数成员
	opts.SetReadPreference(readpref.SecondaryPreferred())                  // 优先读从库
	opts.SetMaxConnIdleTime(time.Duration(cfg.ConnIdleTime) * time.Second) // 设置连接空闲时间 超过就会断开
	return mongo.Connect(context.TODO(), opts)
}

func onExit() {
	loggo.OnExit()
}
