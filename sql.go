// Copyright 2014 The dbrouter Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dbrouter

import (
	"database/sql"
	"fmt"
	"github.com/bitly/go-simplejson"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/shawnfeng/sutil/slog"
	"github.com/shawnfeng/sutil/stime"
	"time"
)

type DB struct {
	*sqlx.DB
}

func (db *DB) NamedExecWrapper(tables []interface{}, query string, arg interface{}) (sql.Result, error) {
	query = fmt.Sprintf(query, tables...)
	return db.DB.NamedExec(query, arg)
}

func (db *DB) NamedQueryWrapper(tables []interface{}, query string, arg interface{}) (*sqlx.Rows, error) {
	query = fmt.Sprintf(query, tables...)
	return db.DB.NamedQuery(query, arg)
}

func (db *DB) SelectWrapper(tables []interface{}, dest interface{}, query string, args ...interface{}) error {
	query = fmt.Sprintf(query, tables...)
	return db.DB.Select(dest, query, args...)
}

func (db *DB) ExecWrapper(tables []interface{}, query string, args ...interface{}) (sql.Result, error) {
	query = fmt.Sprintf(query, tables...)
	return db.DB.Exec(query, args...)
}

func (db *DB) QueryRowxWrapper(tables []interface{}, query string, args ...interface{}) *sqlx.Row {
	query = fmt.Sprintf(query, tables...)
	return db.DB.QueryRowx(query, args...)
}

func (db *DB) QueryxWrapper(tables []interface{}, query string, args ...interface{}) (*sqlx.Rows, error) {
	query = fmt.Sprintf(query, tables...)
	return db.DB.Queryx(query, args...)
}

func (db *DB) GetWrapper(tables []interface{}, dest interface{}, query string, args ...interface{}) error {
	query = fmt.Sprintf(query, tables...)
	return db.DB.Get(dest, query, args...)
}

func NewDB(sqlxdb *sqlx.DB) *DB {
	db := &DB{
		sqlxdb,
	}
	return db
}

type dbSql struct {
	dbType   string
	dbName   string
	dbAddrs  string
	timeOut  time.Duration
	userName string
	passWord string
	db       *DB
}

func (m *dbSql) getType() string {
	return m.dbType
}

func NewdbSql(dbtype, dbname string, cfg []byte) (*dbSql, error) {
	fun := "NewdbSql-->"

	cfg_json, err := simplejson.NewJson(cfg)
	if err != nil {
		return nil, fmt.Errorf("instance db:%s type:%s config:%s unmarshal err:%s", dbname, dbtype, cfg, err)
	}

	addrs, err := cfg_json.Get("addrs").StringArray()
	if err != nil {
		return nil, fmt.Errorf("instance db:%s type:%s config:%s addrs err:%s", dbname, dbtype, cfg, err)
	}

	if len(addrs) != 1 {
		return nil, fmt.Errorf("instance db:%s type:%s config:%s len(addrs)!=1", dbname, dbtype, cfg)
	}

	timeout := 60 * time.Second
	if t, err := cfg_json.Get("timeout").Int64(); err == nil {
		timeout = time.Duration(t) * time.Millisecond
	}

	user, _ := cfg_json.Get("user").String()
	passwd, _ := cfg_json.Get("passwd").String()

	info := &dbSql{
		dbType:   dbtype,
		dbName:   dbname,
		dbAddrs:  addrs[0],
		timeOut:  timeout,
		userName: user,
		passWord: passwd,
	}

	info.db, err = dial(info)
	if err != nil {
		slog.Errorf("%s dbtype:%s dbname:%s cfg:%s", fun, info.dbType, info.dbName, string(cfg))
		return nil, err
	}
	info.db.SetMaxIdleConns(8)
	return info, err
}

func dial(info *dbSql) (db *DB, err error) {
	fun := "dial-->"

	var dataSourceName string
	if info.dbType == DB_TYPE_MYSQL {
		dataSourceName = fmt.Sprintf("%s:%s@tcp(%s)/%s", info.userName, info.passWord, info.dbAddrs, info.dbName)

	} else if info.dbType == DB_TYPE_POSTGRES {
		dataSourceName = fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable",
			info.userName, info.passWord, info.dbAddrs, info.dbName)
	}

	slog.Infof("%s dbtype:%s datasourcename:%s", fun, info.dbType, dataSourceName)
	sqlxdb, err := sqlx.Connect(info.dbType, dataSourceName)
	return NewDB(sqlxdb), err
}

func (m *dbSql) getDB() *DB {
	return m.db
}

func (m *Router) SqlExec(cluster string, query func(*DB, []interface{}) error, tables ...string) error {
	stall := stime.NewTimeStat()
	st := stime.NewTimeStat()
	if len(tables) <= 0 {
		return fmt.Errorf("tables is empty")
	}

	table := tables[0]
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

	dbsql, ok := ins.(*dbSql)
	if !ok {
		return fmt.Errorf("db instance type error: cluster:%s table:%s type:%s", cluster, table, ins.getType())
	}

	durInst := st.Duration()
	st.Reset()

	db := dbsql.getDB()

	defer func() {
		dur := st.Duration()
		m.stat.incQuery(cluster, table, stall.Duration())
		slog.Tracef("[SQL] cls:%s table:%s nmins:%d ins:%d rins:%d query:%d", cluster, table, durInsn, durIns, durInst, dur)
	}()

	var tmptables []interface{}
	for _, item := range tables {
		tmptables = append(tmptables, item)
	}

	return query(db, tmptables)
}

func (m *Router) SqlExecDeprecated(cluster, table string, query func(*sqlx.DB) error) error {
	stall := stime.NewTimeStat()
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

	dbsql, ok := ins.(*dbSql)
	if !ok {
		return fmt.Errorf("db instance type error: cluster:%s table:%s type:%s", cluster, table, ins.getType())
	}

	durInst := st.Duration()
	st.Reset()

	db := dbsql.getDB()

	defer func() {
		m.stat.incQuery(cluster, table, stall.Duration())
		dur := st.Duration()
		slog.Tracef("[SQL] cls:%s table:%s nmins:%d ins:%d rins:%d query:%d", cluster, table, durInsn, durIns, durInst, dur)
	}()

	return query(db.DB)
}
