// Copyright 2014 The dbrouter Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.


package dbrouter


import (
	"log"
	"fmt"
	"time"
	"io/ioutil"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"testing"
)





func TestRouter(t *testing.T) {

	log.Println("test router")

	err := checkVarname("abc")
	fmt.Println("check name:", err)
	if err != nil {
		t.Errorf("check var name error")
	}


	err = checkVarname("abcABC__23")
	fmt.Println("check name:", err)
	if err != nil {
		t.Errorf("check var name error")
	}


	err = checkVarname("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ__0123456789")
	fmt.Println("check name:", err)
	if err != nil {
		t.Errorf("check var name error")
	}



	err = checkVarname("_abcdefg")
	fmt.Println("check name:", err)
	if err == nil {
		t.Errorf("check var name error")
	}


	err = checkVarname("0abcdefg")
	fmt.Println("check name:", err)
	if err == nil {
		t.Errorf("check var name error")
	}


	err = checkVarname("9abcdefg")
	fmt.Println("check name:", err)
	if err == nil {
		t.Errorf("check var name error")
	}


	err = checkVarname("abcdefg*")
	fmt.Println("check name:", err)
	if err == nil {
		t.Errorf("check var name error")
	}


	err = checkVarname("abcdefg[]")
	fmt.Println("check name:", err)
	if err == nil {
		t.Errorf("check var name error")
	}


	newrouter(t)
}

func newrouter(t *testing.T) {
	data, err := ioutil.ReadFile("./router.json")

	if err != nil {
		t.Errorf("config error")
		return
	}

	r, err := NewRouter(data)
	if err != nil {
		t.Errorf("config router error:%s", err)
		return
	}
	log.Println(r, r.dbCls, r.dbIns)


	qf := func(c *mgo.Collection) error {
		log.Printf("do c:%s", c)
		return nil

	}

	err = r.MongoExecEventual("ACCOUNT", "fuck0", qf)
	if err != nil {
		t.Errorf("do error:%s", err)
	}


	err = r.MongoExecEventual("ACCOUNT", "fuck1", qf)
	if err != nil {
		t.Errorf("do error:%s", err)
	}


	err = r.MongoExecMonotonic("ACCOUNT", "fuck2", qf)
	if err != nil {
		t.Errorf("do error:%s", err)
	}


	err = r.MongoExecMonotonic("ACCOUNT", "fuck3", qf)
	if err != nil {
		t.Errorf("do error:%s", err)
	}


	err = r.MongoExecStrong("ACCOUNT", "fuck4", qf)
	if err != nil {
		t.Errorf("do error:%s", err)
	}


	err = r.MongoExecStrong("ACCOUNT", "fuck5", qf)
	if err != nil {
		t.Errorf("do error:%s", err)
	}



	// ===============================
	qf1 := func(c *mgo.Collection) error {
		log.Printf("do c:%s", c)

		_, err := c.Upsert(bson.M{"_id": 3},
			bson.M{"$set": bson.M{"a": time.Now().Unix()}},
		)

		if err != nil {
			t.Errorf("update error:%s", err)
		}
		return err
	}


	for i := 0; i < 12; i++ {

		err = r.MongoExecEventual("ACCOUNT", fmt.Sprintf("fuck%d", i), qf1)
		if err != nil {
			t.Errorf("do error:%s", err)
		}

	}


	for i := 100; i < 110; i++ {

		err = r.MongoExecEventual("ACCOUNT", fmt.Sprintf("fuck%d", i), qf1)
		if err != nil {
			t.Errorf("do error:%s", err)
		}

	}


	err = r.MongoExecEventual("ACCOUNT", fmt.Sprintf("fuck%d", 10000), qf1)
	if err != nil {
		t.Errorf("do error:%s", err)
	}


	log.Println("ROUTER INFO:", r.RouterInfo("ACCOUNT", "fuck10"))
	log.Println("ROUTER INFO:", r.RouterInfo("ACCOUNT", "fuck10000"))
	log.Println("ROUTER INFO:", r.RouterInfo("ACCOUNT", "fuck"))


}

