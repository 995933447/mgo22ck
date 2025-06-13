package main

import (
	"errors"
	"fmt"
	"github.com/995933447/confloader"
	"github.com/995933447/log-go/v2/loggo/logger"
	"runtime"
	"time"
)

type Conf struct {
	Name       string
	MapMgo2Ck  []MapMgo2CkConf
	SysMgoConn MgoConnConf
	Log        logger.LogConf
}

type MapMgo2CkConf struct {
	CkConn          CkConnConf
	MgoConn         MgoConnConf
	SyncSinceSecAgo int64
	Opts            []string
	AllDb           bool
	Dbs             []MgoDbConf
	SkipDbs         []string
	SkipDbPrefixes  []string
}

type MgoDbConf struct {
	Db             string
	Opts           []string
	AllTb          bool
	Tbs            []MgoTbConf
	SkipTbs        []string
	SkipTbPrefixes []string
}

type MgoTbConf struct {
	Tb   string
	Opts []string
}

type CkConnConf struct {
	Host string
	User string
	Pwd  string

	name string
}

func (c CkConnConf) GetName() string {
	c.name = fmt.Sprintf("%s.%s.%s", c.Host, c.User, c.Pwd)
	return c.name
}

type MgoConnConf struct {
	Scheme       string
	Hosts        []string
	SrvUrl       string
	Query        string
	User         string
	Password     string
	PoolNum      int
	MinPoolNum   int
	ConnIdleTime int
	Ssl          bool
	fullUrl      string
}

func (c *MgoConnConf) GetConnUrl() string {
	if c.fullUrl != "" {
		return c.fullUrl
	}

	c.fullUrl = c.SrvUrl

	if c.fullUrl == "" {
		if c.Scheme == "" {
			c.Scheme = "mongodb" // mongodb+srv
		}
		c.fullUrl = c.Scheme + "://" + c.GetUrl()
	} else {
		c.fullUrl = fmt.Sprintf("mongodb+srv://%s:%s@%s", c.User, c.Password, c.SrvUrl)
	}

	if c.ConnIdleTime == 0 {
		c.ConnIdleTime = 30
	}

	if c.PoolNum == 0 {
		c.PoolNum = 128
	}

	return c.fullUrl
}

func (c *MgoConnConf) GetUrl() string {
	add := ""
	for idx, host := range c.Hosts {
		if idx == 0 {
			add = host
		} else {
			add += fmt.Sprintf(",%s", host)
		}
	}
	url := fmt.Sprintf("%s:%s@%s", c.User, c.Password, add)
	if c.Query != "" {
		url = url + "?" + c.Query
	}
	return url
}

var (
	conf       Conf
	initedConf bool
)

func InitConf(fileName string) error {
	if fileName == "" {
		switch runtime.GOOS {
		case "windows":
			fileName = "c://go2ck/go2ck.conf"
		default:
			fileName = "/etc/go2ck/go2ck.conf"
		}
	}

	loader := confloader.NewLoader(fileName, time.Second /* 定时更新配置间隔 */, &conf)
	if err := loader.Load(); err != nil {
		return err
	}

	if len(conf.SysMgoConn.Hosts) == 0 {
		return errors.New("config incorrect,SysMgoConn.Hosts is empty")
	}

	if conf.SysMgoConn.User == "" {
		return errors.New("config incorrect,SysMgoConn.User is empty")
	}

	if conf.Log.File.DefaultLogDir == "" {
		return errors.New("config incorrect,Log.File.DefaultLogDir is empty")
	}

	for i, c := range conf.MapMgo2Ck {
		if len(c.Opts) == 0 {
			return fmt.Errorf("config incorrect,MapMgo2Ck[%d].Opts is empty", i)
		}

		if len(c.MgoConn.Hosts) == 0 {
			return fmt.Errorf("config incorrect,MapMgo2Ck[%d].MgoConn.Hosts is empty", i)
		}

		if c.MgoConn.User == "" {
			return fmt.Errorf("config incorrect,MapMgo2Ck[%d].MgoConn.User is empty", i)
		}

		if c.CkConn.Host == "" {
			return fmt.Errorf("config incorrect,MapCk[%d].CkConn.Host is empty", i)
		}

		if c.CkConn.User == "" {
			return fmt.Errorf("config incorrect,MapCk[%d].CkConn.User is empty", i)
		}
	}

	initedConf = true

	return nil
}

func MustConf() *Conf {
	if !initedConf {
		panic("conf not inited")
	}

	return &conf
}
