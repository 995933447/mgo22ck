package main

import (
	"context"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

const (
	ResumeTokenTbName = "resume_token"
	WatcherErrTbName  = "watcher_err"
)

type BaseModel struct {
	_ID        primitive.ObjectID `bson:"_id,omitempty"`
	CreateTime time.Time          `bson:"create_time"`
	UpdateTime time.Time          `bson:"update_time"`
}

func (b *BaseModel) BeforeWrite() {
	b.CreateTime = time.Now()
	b.UpdateTime = time.Now()
	b._ID = primitive.NewObjectID()
}

type ModelWritable interface {
	BeforeWrite()
}

type ModelWriter[T ModelWritable] struct {
	Data T
}

func (m *ModelWriter[T]) MarshalBSON() ([]byte, error) {
	m.Data.BeforeWrite()
	return bson.Marshal(m.Data)
}

func NewModelWriter[T ModelWritable](data T) *ModelWriter[T] {
	return &ModelWriter[T]{
		Data: data,
	}
}

type ResumeTokenModel struct {
	BaseModel   `bson:",inline"`
	WatcherName string           `bson:"watcher_name"`
	ResumeToken primitive.Binary `bson:"resume_token"`
}

type WatcherErrModel struct {
	BaseModel   `bson:",inline"`
	WatcherName string `bson:"watcher_name"`
	Error       string `bson:"error"`
}

func InitSysRequiredMgo() error {
	mgoConn, err := GetSysMgoConn()
	if err != nil {
		return fmt.Errorf("mgoConn %s not exist", conf.SysMgoConn.GetConnUrl())
	}

	db := mgoConn.Database("mgo2ck")
	// 判断集合是否存在
	collections, err := db.ListCollectionNames(context.TODO(), bson.M{"name": bson.M{"$in": []string{ResumeTokenTbName, WatcherErrTbName}}})
	if err != nil {
		return err
	}

	collectionSet := make(map[string]struct{})
	for _, collection := range collections {
		collectionSet[collection] = struct{}{}
	}

	if _, ok := collectionSet[ResumeTokenTbName]; !ok {
		coll := db.Collection(ResumeTokenTbName)

		indexModel := mongo.IndexModel{
			Keys:    bson.D{{Key: "watcher_name", Value: 1}},
			Options: options.Index().SetUnique(true),
		}

		_, err = coll.Indexes().CreateOne(context.TODO(), indexModel)
		if err != nil {
			return err
		}
	}

	if _, ok := collectionSet[WatcherErrTbName]; !ok {
		coll := db.Collection(WatcherErrTbName)

		indexModel := mongo.IndexModel{
			Keys: bson.D{{Key: "watcher_name", Value: 1}},
		}

		_, err = coll.Indexes().CreateOne(context.TODO(), indexModel)
		if err != nil {
			return err
		}
	}

	return nil
}

func GetSysMgoConn() (*mongo.Client, error) {
	cfg := MustConf()
	mgoConn, ok := mgoConns[cfg.SysMgoConn.GetConnUrl()]
	if !ok {
		var err error
		mgoConn, err = NewMgoConn(cfg.SysMgoConn)
		if err != nil {
			return nil, err
		}
	}
	return mgoConn, nil
}

func RecWatcherErr(watcherName string, err error) error {
	mgoConn, e := GetSysMgoConn()
	if e != nil {
		return e
	}

	_, e = mgoConn.Database("mgo2ck").Collection(WatcherErrTbName).InsertOne(context.TODO(), NewModelWriter(&WatcherErrModel{
		WatcherName: watcherName,
		Error:       err.Error(),
	}))
	if e != nil {
		return e
	}

	return nil
}

func RecResumeToken(watcherName string, resumeToken []byte) error {
	mgoConn, err := GetSysMgoConn()
	if err != nil {
		return err
	}

	token, err := GetResumeToken(watcherName)
	if err != nil {
		return err
	}

	coll := mgoConn.Database("mgo2ck").Collection(ResumeTokenTbName)
	if token == nil {
		_, err = coll.InsertOne(context.TODO(), NewModelWriter(&ResumeTokenModel{
			WatcherName: watcherName,
			ResumeToken: primitive.Binary{Subtype: 0x00, Data: resumeToken},
		}))
		if err != nil {
			return err
		}
	}

	_, err = coll.UpdateOne(context.TODO(), bson.M{"watcher_name": watcherName}, bson.M{
		"$set": bson.M{"resume_token": resumeToken},
	})
	if err != nil {
		return err
	}

	return nil
}

func GetResumeToken(watcherName string) (bson.Raw, error) {
	mgoConn, err := GetSysMgoConn()
	if err != nil {
		return nil, err
	}

	var resumeToken ResumeTokenModel

	err = mgoConn.Database("mgo2ck").Collection(ResumeTokenTbName).FindOne(context.TODO(), bson.M{
		"watcher_name": watcherName,
	}).Decode(&resumeToken)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, err
	}

	return resumeToken.ResumeToken.Data, nil
}
