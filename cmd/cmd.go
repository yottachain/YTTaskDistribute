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
	"math/rand"
	"sync"
	"time"
)

func main(){
	var listenaddr string
	var runtime int
	var times int
	var timeout int
	var dstlen int

	flag.StringVar(&listenaddr, "l", "/ip4/0.0.0.0/tcp/9003", "监听地址")
	flag.IntVar(&runtime, "rt", 60, "测试时间，默认是60s(1分钟)")
	flag.IntVar(&times, "es", 2, "每个矿机每秒上传的分片数")
	flag.IntVar(&timeout, "ot", 10, "超时时间, 默认10秒")
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

	log.Infof("peer id : %s", peer.Encode(hst.Config().ID))

	dl := &dn.DnList{Sdnlist: make([]*dn.AddInfo, 0), Ddnlist:make([]*dn.AddInfo, 0), L:sync.Mutex{}}
	Nst.Init()
	go Nst.TimerPrint()

	wg := sync.WaitGroup{}
	startTime := time.Now()
	dl.UpdateList()
	for {
		if int(time.Now().Sub(startTime).Seconds()) > runtime {
			break
		}

		<-time.After(time.Second*1)
		if dl.IsAvalible() {
			for i, v := range dl.Ddnlist {
				if i >= dstlen {
					break
				}
				go upload(hst, v, &wg, 5, times, timeout, Nst)
			}
		}
	}

	wg.Wait()
	Nst.Print()
}

func upload(hst hi.Host, dAddr *dn.AddInfo, wg *sync.WaitGroup, connTrys int, ups int, ot int, nst *NodeStat) {
	var clt *client.YTHostClient = nil
	var err error
	for i := 0; i < connTrys; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(ot))

		clt, err = hst.ClientStore().Get(ctx, dAddr.NodeID, dAddr.Addrs)
		if err != nil {
			ADDRs := make([]string, len(dAddr.Addrs))
			for k, m := range dAddr.Addrs {
				ADDRs[k] = m.String()
			}
			var sAddrs string
			for _, v := range ADDRs {
				sAddrs = sAddrs + v + " "
			}
			log.WithFields(log.Fields{
				"nodeid": peer.Encode(dAddr.NodeID),
				"addrs":  sAddrs,
			}).Errorf("connect error=%s", err.Error())

			cancel()
			continue
		}
		cancel()
		break
	}

	if clt == nil {
		return
	}

	for i := 0; i < ups; i++ {
		wg.Add(1)
		go upShard(clt, dAddr.Id, ot, wg, nst)
	}
}

func upShard(clt *client.YTHostClient, minerId int, timeOut int, wg *sync.WaitGroup, nst *NodeStat) (err error) {
	defer func() {
		wg.Done()
	}()

	var getToken message.NodeCapacityRequest
	getTokenData, err := proto.Marshal(&getToken)
	if err != nil {
		log.WithFields(log.Fields{
			"minerId": minerId,
			"err": err,
		}).Error("request proto marshal error")

		return
	}

	nst.GtsAdd(minerId)
	ctx1, cal := context.WithTimeout(context.Background(), time.Second*1)
	defer cal()
	res, err := clt.SendMsg(ctx1, message.MsgIDNodeCapacityRequest.Value(), getTokenData)
	if err != nil {
		log.WithFields(log.Fields{
			"minerId": minerId,
			"error": err.Error(),
		}).Error("get token fail")

		return
	}

	var resGetToken message.NodeCapacityResponse
	err = proto.Unmarshal(res[2:], &resGetToken)
	if err != nil {
		log.WithFields(log.Fields{
			"minerId": minerId,
			"err": err.Error(),
		}).Error("get token response proto Unmarshal error")
		return
	}

	if !resGetToken.Writable  {
		log.WithFields(log.Fields{
			"minerId": minerId,
		}).Error("token unavailable")
		return
	}else {
		log.WithFields(log.Fields{
			"minerId": minerId,
		}).Error("get token ok!")
		nst.GtSucAdd(minerId)
	}

	startTime := time.Now()
	var uploadReqMsg message.UploadShardRequestTest
	uploadReqMsg.AllocId = resGetToken.AllocId
	uploadReqMsg.DAT = make([]byte, 16*1024)
	rand.Read(uploadReqMsg.DAT)
	uploadReqMsg.Sleep = 100

	uploadReqData, err := proto.Marshal(&uploadReqMsg)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err.Error(),
			"minerId": minerId,
		}).Error("upload shard request proto marshal error")
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second*time.Duration(timeOut))
	defer cancel2()
	res, err = clt.SendMsg(ctx2, message.MsgIDSleepReturn.Value(), uploadReqData)
	if err != nil {
		log.WithFields(log.Fields{
			"minerId": minerId,
			"err":    err.Error(),
		}).Error("message send error")

		nst.UpDlyAdd(minerId, time.Now().Sub(startTime).Milliseconds())
		return
	}

	var resmsg message.UploadShardResponse
	err = proto.Unmarshal(res[2:], &resmsg)
	if err != nil {
		log.WithFields(log.Fields{
			"minerId": minerId,
			"err": err.Error(),
		}).Error("msg response proto Unmarshal error")
	}else {
		log.WithFields(log.Fields{
			"minerId": minerId,
		}).Error("up shard OK!")
		nst.UpSucAdd(minerId)
		nst.UpDlyAdd(minerId, time.Now().Sub(startTime).Milliseconds())
	}

	return
}
