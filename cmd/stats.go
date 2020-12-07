package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

type StatShard struct {
	Gts	int64
	GtSucs	int64
	UpSucs	int64
	UpDlyAvg int64
	UpTotalDly int64
	sync.Mutex
}

func (st *StatShard) GtsAdd(){
	st.Lock()
	defer st.Unlock()
	st.Gts++
}

func (st *StatShard) GtSucAdd(){
	st.Lock()
	defer st.Unlock()
	st.GtSucs++
}

func (st *StatShard) UpSucAdd(){
	st.Lock()
	defer st.Unlock()
	st.UpSucs++
}

func (st *StatShard) UpDlyAdd(dly int64){
	st.Lock()
	defer st.Unlock()
	st.UpTotalDly += dly
	st.UpDlyAvg = st.UpTotalDly / st.UpSucs
}

type NodeStat struct {
	sync.Map
	sync.Mutex
	fd *os.File
}

var Nst = &NodeStat{sync.Map{}, sync.Mutex{}, nil}

func (ns *NodeStat) GtsAdd(id int) {
	ns.Lock()
	defer ns.Unlock()
	st, ok := ns.Map.Load(id)
	if !ok  {
		st = &StatShard{}
		ns.Map.LoadOrStore(id, st)
	}

	st.(*StatShard).GtsAdd()
}

func (ns *NodeStat) GtSucAdd(id int) {
	ns.Lock()
	defer ns.Unlock()
	st, ok := ns.Map.Load(id)
	if !ok  {
		st = &StatShard{}
		ns.Map.LoadOrStore(id, st)
	}

	st.(*StatShard).GtSucAdd()
}

func (ns *NodeStat) UpSucAdd(id int) {
	ns.Lock()
	defer ns.Unlock()
	st, ok := ns.Map.Load(id)
	if !ok  {
		st = &StatShard{}
		ns.Map.LoadOrStore(id, st)
	}

	st.(*StatShard).UpSucAdd()
}

func (ns *NodeStat) UpDlyAdd(id int, dly int64) {
	ns.Lock()
	defer ns.Unlock()
	st, ok := ns.Map.Load(id)
	if !ok  {
		st = &StatShard{}
		ns.Map.LoadOrStore(id, st)
	}

	st.(*StatShard).UpDlyAdd(dly)
}

func (ns *NodeStat) Init() {
	fd, err := os.OpenFile("upstat.log", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		log.Fatalln("open upstat.log fail  "+ err.Error())
	}
	ns.fd = fd
}

func (ns *NodeStat) TimerPrint() {
	for {
		<-time.After(time.Second*2)

		f := func(k, v interface{}) bool {
			ns.Lock()
			_, _ = fmt.Fprintf(ns.fd, "dstId=%d gts=%d gtsucs=%d upsucs=%d updly=%d\n",
				k, v.(*StatShard).Gts, v.(*StatShard).GtSucs, v.(*StatShard).UpSucs, v.(*StatShard).UpDlyAvg)
			ns.Unlock()

			return true
		}

		ns.Map.Range(f)
	}
}

func (ns *NodeStat) Print() {
	f := func(k, v interface{}) bool {
		ns.Lock()
		_, _ = fmt.Fprintf(ns.fd, "dstId=%d gtsucs=%d upsucs=%d updly=%d\n",
					k, v.(*StatShard).GtSucs, v.(*StatShard).UpSucs, v.(*StatShard).UpDlyAvg)
		ns.Unlock()

		return true
	}

	ns.Map.Range(f)

	_ = ns.fd.Close()
	return
}
