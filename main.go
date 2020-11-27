package main

import (
	"context"
	"flag"
	"github.com/gogo/protobuf/proto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	log "github.com/sirupsen/logrus"
	"github.com/yottachain/YTDataNode/message"
	host "github.com/yottachain/YTHost"
	hi "github.com/yottachain/YTHost/interface"
	"github.com/yottachain/YTHost/option"
	"github.com/yottachain/YTTaskDistribute/dn"
	"github.com/yottachain/YTTaskDistribute/stat"
	"sync"
	"time"
)

func main () {
	var listenaddr string
	var runtime int
	var Type int

	flag.StringVar(&listenaddr, "l", "/ip4/0.0.0.0/tcp/9003", "监听地址")
	flag.IntVar(&runtime, "rt", 60, "测试时间，默认是60s(1分钟)")
	flag.IntVar(&Type, "tt", 2, "测试类型 0上传 1下载 2上传和下载")
	flag.Parse()

	lsma, err := multiaddr.NewMultiaddr(listenaddr)
	if err != nil {
		log.Fatal(err)
	}
	hst, err := host.NewHost(option.ListenAddr(lsma), option.OpenPProf("0.0.0.0:10000"))
	if err != nil {
		log.Fatal(err)
	}

	dl := &dn.DnList{Sdnlist: make([]*dn.AddInfo, 0), Ddnlist:make([]*dn.AddInfo, 0), L:sync.Mutex{}}

	nst := stat.Nst
	nst.Init()

	wg := sync.WaitGroup{}
	startTime := time.Now()
	for {
		if int(time.Now().Sub(startTime).Seconds()) > runtime {
			break
		}
		dl.UpdateList()
		if dl.IsAvalible() {
			dl.L.Lock()
			for _, v := range dl.Sdnlist {
				for _, v1 := range dl.Ddnlist {
					if Type > 2 || Type < 0 {
						log.Panic("测试类型错误")
					}
					if Type == 2 {
						wg.Add(1)
						go testPerf(hst, v, v1, 0, nst, &wg)
						wg.Add(1)
						go testPerf(hst, v, v1, 1, nst, &wg)
					}else {
						wg.Add(1)
						go testPerf(hst, v, v1, int32(Type), nst, &wg)
					}
				}
			}
			dl.L.Unlock()
		}
	}

	wg.Wait()
	nst.Print()
}

func testPerf(hst hi.Host, sAddr *dn.AddInfo, dAddr *dn.AddInfo, Type int32, nst *stat.NodeStat, wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(10))
	defer cancel()
	clt, err := hst.ClientStore().Get(ctx, sAddr.NodeID, sAddr.Addrs)
	if err != nil {
		ADDRs := make([]string, len(sAddr.Addrs))
		for k, m := range sAddr.Addrs {
			ADDRs[k] = m.String()
		}
		var sAddrs string
		for _, v := range ADDRs {
			sAddrs = sAddrs + v + " "
		}
		log.WithFields(log.Fields{
			"nodeid": peer.Encode(sAddr.NodeID),
			"addrs":  sAddrs,
		}).Error("connect error=", err)

		return
	}

	var msgReq message.TestMinerPerfTask
	msgReq.TestType = Type
	for _, v := range dAddr.Saddrs {
		p2pAddr := v + "/p2p/" + peer.Encode(dAddr.NodeID)
		log.Infof("p2p addr: %s", p2pAddr)
		msgReq.TargetMa = append(msgReq.TargetMa, p2pAddr)
	}
	msgReqData, err := proto.Marshal(&msgReq)
	if err != nil {
		log.Error("req marshal error")
		return
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second*time.Duration(30))
	defer cancel2()
	res, err := clt.SendMsg(ctx2, message.MsgIDTestMinerPerfTask.Value(), msgReqData)
	if err != nil {
		log.WithFields(log.Fields{
			"nodeid": peer.Encode(sAddr.NodeID),
			"type" : Type,
		}).Errorf("send error=", err)

		return
	}

	var msgRsp message.TestMinerPerfTaskRes
	err = proto.Unmarshal(res, &msgRsp)
	if err != nil {
		log.Error("rsp Unmarshal error")
		return
	}

	nst.Dly(stat.IDs{sAddr.Id, dAddr.Id}, msgRsp.Latency/1000000, int(Type))
}
