package stat

import (
	"fmt"
	"log"
	"os"
	"sync"
)

type StatDly struct {
	upSucDlyTotal int64
	upErrDlyTotal int64
	upTimes		int
	upSucDlyAvg	int64
	upErrDlyAvg	int64

	upDlyTotal int64
	upDlyAvg int64

	dwSucDlyTotal int64
	dwErrDlyTotal int64
	dwTimes		int
	dwSucDlyAvg 	int64
	dwErrDlyAvg 	int64

	SrcConnerrTimes 	int
	SendTaskErrTimes    int

	uptaskSuccs			int64
	uptaskFails 		int64
	upSucRate 			float32

	dwtaskSuccs			int64
	dwtaskFails 		int64
	dwSucRate  			float32

	upExecTime			int64
	dwExecTime 			int64

	sync.Mutex
}

func (st *StatDly) UpDelay(totalDly int64, Sucdelay int64, Errdelay int64, exectime int64){
	st.Lock()
	defer st.Unlock()
	st.upTimes++
	st.upSucDlyTotal = st.upSucDlyTotal + Sucdelay
	st.upErrDlyTotal = st.upErrDlyTotal + Errdelay
	st.upSucDlyAvg = st.upSucDlyTotal/int64(st.upTimes)
	st.upErrDlyAvg = st.upErrDlyTotal/int64(st.upTimes)

	st.upDlyTotal +=  totalDly
	st.upDlyAvg =  st.upDlyTotal/int64(st.upTimes)

	st.upExecTime += exectime
}

func (st *StatDly) DwDelay(totalDly int64, Sucdelay int64, Errdelay int64, exectime int64){
	st.Lock()
	defer st.Unlock()
	st.dwTimes++
	st.dwSucDlyTotal = st.dwSucDlyTotal + Sucdelay
	st.dwErrDlyTotal = st.dwErrDlyTotal + Errdelay
	st.dwSucDlyAvg = st.dwSucDlyTotal/int64(st.dwTimes)
	st.dwErrDlyAvg = st.dwErrDlyTotal/int64(st.dwTimes)

	st.dwExecTime += exectime
}

func (st *StatDly) SrcConnErrAdd(){
	st.Lock()
	defer st.Unlock()
	st.SrcConnerrTimes++
}

func (st *StatDly) SendTaskErrAdd(){
	st.Lock()
	defer st.Unlock()
	st.SendTaskErrTimes++
}

func (st *StatDly) TaskExeCounts(succs int64, fails int64, Type int){
	st.Lock()
	defer st.Unlock()
	if Type == 0 {
		st.uptaskSuccs = st.uptaskSuccs + succs
		st.uptaskFails = st.uptaskFails + fails
	}else if Type == 1 {
		st.dwtaskSuccs = st.dwtaskSuccs + succs
		st.dwtaskFails = st.dwtaskFails + fails
	}

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

func (ns *NodeStat) Dly(ids IDs, totalDly int64, Sucdly int64, Errdly int64, exetime int64, Type int) {
	ns.Lock()
	defer ns.Unlock()
	st, ok := ns.Map.Load(ids)
	if !ok  {
		st = &StatDly{0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, sync.Mutex{}}
		ns.Map.LoadOrStore(ids, st)
	}

	if Type == 0 {
		st.(*StatDly).UpDelay(totalDly, Sucdly, Errdly, exetime)
	}else if Type == 1 {
		st.(*StatDly).DwDelay(totalDly, Sucdly, Errdly, exetime)
	}
}

func (ns *NodeStat) Counts(ids IDs, succs int64, fails int64, Type int) {
	ns.Lock()
	defer ns.Unlock()
	st, ok := ns.Map.Load(ids)
	if !ok  {
		st = &StatDly{0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0,0,
			0, 0, 0, 0, 0, 0, 0, 0, sync.Mutex{}}
		ns.Map.LoadOrStore(ids, st)
	}

	st.(*StatDly).TaskExeCounts(succs, fails, Type)
}

func (ns *NodeStat) Err(ids IDs, Type int) {
	ns.Lock()
	defer ns.Unlock()
	st, ok := ns.Map.Load(ids)
	if !ok  {
		st = &StatDly{0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, sync.Mutex{}}
		ns.Map.LoadOrStore(ids, st)
	}

	if Type == 0 {
		st.(*StatDly).SrcConnErrAdd()
	}else if Type == 1 {
		st.(*StatDly).SendTaskErrAdd()
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
		ns.Lock()
		var upRate float32 = 0
		upTotals := v.(*StatDly).uptaskSuccs + v.(*StatDly).uptaskFails
		if upTotals != 0 {
			upRate = float32(v.(*StatDly).uptaskSuccs) / float32(upTotals)
		}

		var dwRate float32 = 0
		dwTotals := v.(*StatDly).dwtaskSuccs + v.(*StatDly).dwtaskFails
		if upTotals != 0 {
			dwRate = float32(v.(*StatDly).dwtaskSuccs) / float32(dwTotals)
		}

		_, _ = fmt.Fprintf(ns.fd, "srcId=%d dstId=%d " +
					"upTaskTimes=%d upExeTime=%d upDly=%d upSucRate=%f upSucDly=%d uptaskSuccs=%d upErrDly=%d uptaskFails=%d " +
					"dwTaskTimes=%d dwExeTime=%d dwSucRate=%f dwSucDly=%d dwtaskSuccs=%d dwErrDly=%d dwtaskFails=%d " +
					"SconnErr=%d SendTaskErr=%d\n",
			k.(IDs).Sid, k.(IDs).Did,
			v.(*StatDly).upTimes, v.(*StatDly).upExecTime, v.(*StatDly).upDlyAvg, upRate, v.(*StatDly).upSucDlyAvg, v.(*StatDly).uptaskSuccs, v.(*StatDly).upErrDlyAvg, v.(*StatDly).uptaskFails,
			v.(*StatDly).dwTimes,  v.(*StatDly).dwExecTime, dwRate, v.(*StatDly).dwSucDlyAvg, v.(*StatDly).dwtaskSuccs,  v.(*StatDly).dwErrDlyAvg, v.(*StatDly).dwtaskFails,
			v.(*StatDly).SrcConnerrTimes, v.(*StatDly).SendTaskErrTimes)
		ns.Unlock()

		return true
	}

	ns.Map.Range(f)

	_ = ns.fd.Close()
	return
}