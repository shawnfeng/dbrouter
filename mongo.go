// Copyright 2014 The dbrouter Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dbrouter

import (
	"fmt"
	"sync"
	"time"
	//"reflect"
	"gopkg.in/mgo.v2"
	//"gopkg.in/mgo.v2/bson"

	"github.com/bitly/go-simplejson"

	"github.com/shawnfeng/sutil/slog"
	"github.com/shawnfeng/sutil/stime"
)

type dbMongo struct {
	dbType   string
	dbName   string
	dialInfo *mgo.DialInfo

	sessMu  sync.RWMutex
	session [3]*mgo.Session
}

func (m *dbMongo) getType() string {
	return m.dbType
}

func NewdbMongo(dbtype, dbname string, cfg []byte) (*dbMongo, error) {

	cfg_json, err := simplejson.NewJson(cfg)
	if err != nil {
		return nil, fmt.Errorf("instance db:%s type:%s config:%s unmarshal err:%s", dbname, dbtype, cfg, err)
	}

	addrs, err := cfg_json.Get("addrs").StringArray()
	if err != nil {
		return nil, fmt.Errorf("instance db:%s type:%s config:% addrs err:%s", dbname, dbtype, cfg, err)
	}

	timeout := 60 * time.Second
	if t, err := cfg_json.Get("timeout").Int64(); err == nil {
		timeout = time.Duration(t) * time.Millisecond
	}

	user, _ := cfg_json.Get("user").String()
	passwd, _ := cfg_json.Get("passwd").String()

	info := &mgo.DialInfo{
		Addrs:    addrs,
		Timeout:  timeout,
		Database: dbname,
		Username: user,
		Password: passwd,
	}

	return &dbMongo{
		dbType:   dbtype,
		dbName:   dbname,
		dialInfo: info,
	}, nil

}

type mode int

const (
	eventual  mode = 0
	monotonic mode = 1
	strong    mode = 2
)

func dialConsistency(info *mgo.DialInfo, consistency mode) (session *mgo.Session, err error) {

	// http://godoc.org/gopkg.in/mgo.v2#Dial
	// This method is generally called just once for a given cluster.
	// Further sessions to the same cluster are then established using the New or Copy methods on the obtained session.
	// This will make them share the underlying cluster, and manage the pool of connections appropriately.
	// Once the session is not useful anymore, Close must be called to release the resources appropriately.
	session, err = mgo.DialWithInfo(info)
	if err != nil {
		return
	}
	// 看Dial内部的实现
	session.SetSyncTimeout(1 * time.Minute)
	// 不设置这个在执行写入，表不存在时候会报 read tcp 127.0.0.1:27017: i/o timeout
	session.SetSocketTimeout(1 * time.Minute)

	switch consistency {
	case eventual:
		session.SetMode(mgo.Eventual, true)
	case monotonic:
		session.SetMode(mgo.Monotonic, true)
	case strong:
		session.SetMode(mgo.Strong, true)
	}

	return
}

func dialConsistencyWithUrl(url string, timeout time.Duration, consistency mode) (session *mgo.Session, err error) {

	session, err = mgo.DialWithTimeout(url, timeout)
	if err != nil {
		return
	}
	// 看Dial内部的实现
	session.SetSyncTimeout(1 * time.Minute)
	// 不设置这个在执行写入，表不存在时候会报 read tcp 127.0.0.1:27017: i/o timeout
	session.SetSocketTimeout(1 * time.Minute)

	switch consistency {
	case eventual:
		session.SetMode(mgo.Eventual, true)
	case monotonic:
		session.SetMode(mgo.Monotonic, true)
	case strong:
		session.SetMode(mgo.Strong, true)
	}

	return
}

func (m *dbMongo) checkGetSession(consistency mode) *mgo.Session {
	m.sessMu.RLock()
	defer m.sessMu.RUnlock()

	return m.session[consistency]

}

func (m *dbMongo) initSession(consistency mode) (*mgo.Session, error) {
	m.sessMu.Lock()
	defer m.sessMu.Unlock()
	//fmt.Println("CCCCCC", m.session)

	if m.session[consistency] != nil {
		return m.session[consistency], nil
	} else {
		s, err := dialConsistency(m.dialInfo, consistency)
		if err != nil {
			return nil, err
		} else {
			m.session[consistency] = s
			return m.session[consistency], nil
		}
	}
}

func (m *dbMongo) getSession(consistency mode) (*mgo.Session, error) {
	if s := m.checkGetSession(consistency); s != nil {
		return s, nil
	} else {
		return m.initSession(consistency)
	}
}

func (m *Router) mongoExec(consistency mode, cluster, table string, query func(*mgo.Collection) error) error {
	st := stime.NewTimeStat()

	ins_name := m.dbCls.getInstance(cluster, table)
	if ins_name == "" {
		return fmt.Errorf("cluster instance not find: cluster:%s table:%s", cluster, table)
	}

	durInsn := st.Duration()
	st.Reset()

	ins := m.dbIns.get(ins_name)
	if ins == nil {
		return fmt.Errorf("db instance not find: cluster:%s table:%s", cluster, table)
	}

	durIns := st.Duration()
	st.Reset()

	db, ok := ins.(*dbMongo)
	if !ok {
		return fmt.Errorf("db instance type error: cluster:%s table:%s type:%s", cluster, table, ins.getType())
	}

	durInst := st.Duration()
	st.Reset()

	sess, err := db.getSession(consistency)
	if err != nil {
		return err
	}

	if sess == nil {
		return fmt.Errorf("db instance session empty: cluster:%s table:%s type:%s", cluster, table, ins.getType())
	}

	durSess := st.Duration()
	st.Reset()

	sessionCopy := sess.Copy()
	defer sessionCopy.Close()
	c := sessionCopy.DB("").C(table)

	durcopy := st.Duration()
	st.Reset()

	defer func() {
		dur := st.Duration()
		m.stat.incQuery(cluster, table)
		slog.Tracef("[MONGO] const:%d cls:%s table:%s nmins:%d ins:%d rins:%d sess:%d copy:%d query:%d", consistency, cluster, table, durInsn, durIns, durInst, durSess, durcopy, dur)
	}()

	return query(c)
}

func (m *Router) MongoExecEventual(cluster, table string, query func(*mgo.Collection) error) error {
	return m.mongoExec(eventual, cluster, table, query)
}

func (m *Router) MongoExecMonotonic(cluster, table string, query func(*mgo.Collection) error) error {
	return m.mongoExec(monotonic, cluster, table, query)
}

func (m *Router) MongoExecStrong(cluster, table string, query func(*mgo.Collection) error) error {
	return m.mongoExec(strong, cluster, table, query)
}
