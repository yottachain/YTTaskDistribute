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
	"github.com/yottachain/YTHost/client"
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
	var times int
	var taskTime int
	var timeout int
	var srclen int
	var dstlen int

	flag.StringVar(&listenaddr, "l", "/ip4/0.0.0.0/tcp/9003", "监听地址")
	flag.IntVar(&runtime, "rt", 60, "测试时间，默认是60s(1分钟)")
	flag.IntVar(&Type, "tt", 2, "测试类型 0上传 1下载 2上传和下载")
	flag.IntVar(&times, "es", 1, "执行的次数")
	flag.IntVar(&taskTime, "tet", 600, "任务的执行时间, 默认600秒")
	flag.IntVar(&timeout, "ot", 10, "超时时间, 默认10秒")
	flag.IntVar(&srclen, "sl", 1000, "使用源地址的数量")
	flag.IntVar(&dstlen, "dl", 1, "使用目的地址的数量")
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
	var ts = 1
	for {
		if int(time.Now().Sub(startTime).Seconds()) > runtime || ts > times {
			break
		}
		dl.UpdateList()
		if dl.IsAvalible() {
			ts++
			dl.L.Lock()
			log.WithFields(log.Fields{
				"slistLen": len(dl.Sdnlist),
				"distLen": len(dl.Ddnlist),
			}).Info("list info")

			for i, v := range dl.Sdnlist {
				if i >= srclen {
					break
				}
				for j, v1 := range dl.Ddnlist {
					if Type > 2 || Type < 0 {
						log.Panic("测试类型错误")
					}
					if j >= dstlen {
						break
					}
					if Type == 2 {
						wg.Add(1)
						go testPerf(hst, v, v1, 0, nst, &wg, taskTime, 5, timeout)
						wg.Add(1)
						go testPerf(hst, v, v1, 1, nst, &wg, taskTime, 5, timeout)
					}else {
						wg.Add(1)
						go testPerf(hst, v, v1, int32(Type), nst, &wg, taskTime, 5, timeout)
					}
				}
			}
			dl.L.Unlock()
		}
	}

	wg.Wait()
	nst.Print()
}

func testPerf(hst hi.Host, sAddr *dn.AddInfo, dAddr *dn.AddInfo, Type int32, nst *stat.NodeStat, wg *sync.WaitGroup,
	taskTime int, connTrys int, ot int) {
	defer func() {
		wg.Done()
	}()

	var clt *client.YTHostClient = nil
	var err error
	for i := 0; i < connTrys; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(ot))

		clt, err = hst.ClientStore().Get(ctx, sAddr.NodeID, sAddr.Addrs)
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
			}).Errorf("connect error=%s", err.Error())

			nst.Err(stat.IDs{sAddr.Id, dAddr.Id}, 0)

			cancel()
			continue
		}
		cancel()
		break
	}

	if clt == nil {
		return
	}

	var msgReq message.TestMinerPerfTask
	msgReq.TestType = Type
	msgReq.TestTime = int64(taskTime)
	msgReq.TimeOut = int64(ot)

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

	startTime := time.Now()
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second*time.Duration(taskTime+60))
	defer cancel2()
	res, err := clt.SendMsg(ctx2, message.MsgIDTestMinerPerfTask.Value(), msgReqData)
	if err != nil {
		log.WithFields(log.Fields{
			"nodeid": peer.Encode(sAddr.NodeID),
			"type" : Type,
		}).Errorf("send error=%s", err.Error())

		nst.Err(stat.IDs{sAddr.Id, dAddr.Id}, 1)

		return
	}
	exeTotalTime := time.Now().Sub(startTime).Seconds()

	var msgRsp message.TestMinerPerfTaskRes
	err = proto.Unmarshal(res, &msgRsp)
	if err != nil {
		log.Error("rsp Unmarshal error")
		return
	}

	var sucDly int64 = 0
	var errDly int64 = 0
	var totalDly int64 = 0

	if msgRsp.SuccessCount != 0 {
		log.Infof("SuccessLatency %d", msgRsp.SuccessLatency)
		sucDly = msgRsp.SuccessLatency/msgRsp.SuccessCount
	}

	if msgRsp.ErrorCount != 0 {
		log.Infof("ErrorLatency %d", msgRsp.ErrorLatency)
		errDly = msgRsp.ErrorLatency/msgRsp.ErrorCount
	}

	totalCount := msgRsp.SuccessCount + msgRsp.ErrorCount
	if totalCount != 0 {
		totalDly = (msgRsp.SuccessLatency+ msgRsp.ErrorLatency)/totalCount
	}


	nst.Dly(stat.IDs{sAddr.Id, dAddr.Id}, totalDly, sucDly, errDly, int64(exeTotalTime), int(Type))
	nst.Counts(stat.IDs{sAddr.Id, dAddr.Id}, msgRsp.SuccessCount, msgRsp.ErrorCount, int(Type))
}
