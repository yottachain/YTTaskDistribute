package stat

import (
	"fmt"
	"log"
	"os"
	"sync"
)

type StatDly struct {
	upDlyTotal int64
	upTimes		int
	upDlyAvg	int64
	dwDlyTotal int64
	dwTimes		int
	dwDlyAvg 	int64

	sync.Mutex
}

func (st *StatDly) UpDelay(delay int64){
	st.Lock()
	defer st.Unlock()
	st.upDlyTotal = st.upDlyTotal + delay
	st.upTimes++
	st.upDlyAvg = st.upDlyTotal/int64(st.upTimes)
}

func (st *StatDly) DwDelay(delay int64){
	st.Lock()
	defer st.Unlock()
	st.dwDlyTotal = st.dwDlyTotal + delay
	st.dwTimes++
	st.dwDlyAvg = st.dwDlyTotal/int64(st.dwTimes)
}

type NodeStat struct {
	sync.Map
	sync.Mutex
	fd *os.File
}

var Nst = &NodeStat{sync.Map{}, sync.Mutex{}, nil}

type IDs struct {
	Sid   int
	Did   int
}

func (ns *NodeStat) Dly(ids IDs, dly int64, Type int) {
	ns.Lock()
	defer ns.Unlock()
	st, ok := ns.Map.Load(ids)
	if !ok  {
		st = &StatDly{0, 0, 0, 0, 0, 0, sync.Mutex{}}
		ns.Map.LoadOrStore(ids, st)
	}

	if Type == 0 {
		st.(*StatDly).UpDelay(dly)
	}else if Type == 1 {
		st.(*StatDly).DwDelay(dly)
	}
}

func (ns *NodeStat) Init() {
	fd, err := os.OpenFile("stat.log", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		log.Fatalln("open stat.log fail  "+ err.Error())
	}
	ns.fd = fd
}

func (ns *NodeStat) Print() {
	f := func(k, v interface{}) bool {
		_, _ = fmt.Fprintf(ns.fd, "srcId=%d dstId=%d upDly=%d dwDly=%d\n",
			k.(IDs).Sid, k.(IDs).Did, v.(*StatDly).upDlyAvg, v.(*StatDly).dwDlyAvg,)

		return true
	}

	ns.Map.Range(f)

	_ = ns.fd.Close()
	return
}