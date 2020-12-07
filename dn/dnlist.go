package dn

import (
	"bufio"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/sirupsen/logrus"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type dnInfo struct {
	IP []string `json:"ip"`
	ID string `json:"id"`
	NodeID string `json:"nodeid"`
}

type AddInfo struct {
	Id        int
	NodeID    peer.ID
	Addrs 	  []multiaddr.Multiaddr
	Saddrs    [] string
}

func GetCurrentPath() string {
	file, _ := exec.LookPath(os.Args[0])
	if file == "" {
		ApplicationPath, _ := filepath.Abs(file)
		return ApplicationPath
	} else {
		fi, err := os.Lstat(file)
		if err != nil {
			log.Panicf("GetCurrentPath.Lstat ERR:%s\n ", err)
		}
		if fi.Mode()&os.ModeSymlink == os.ModeSymlink {
			execPath, err := os.Readlink(file)
			if err != nil {
				log.Panicf("GetCurrentPath.Readlink ERR:%s\n ", err)
			}
			ApplicationPath, _ := filepath.Split(execPath)
			return ApplicationPath
		} else {
			ApplicationPath, _ := filepath.Split(file)
			return ApplicationPath
		}
	}
}

func addrsToMaddrs(addrs [] string) []multiaddr.Multiaddr {
	if len(addrs) <= 0 {
		return nil
	}

	var mas = make([]multiaddr.Multiaddr, len(addrs))
	for k, v := range addrs {
		//logrus.Printf("add:%s", v)
		ma, err := multiaddr.NewMultiaddr(v)
		if err != nil {
			logrus.Infof("transform error:%s", err.Error())
			continue
		}
		mas[k] = ma
	}

	return  mas
}

func GetDnlist(path string) [] *AddInfo {
	addlist := make([] *AddInfo, 0)
	var Map sync.Map

	f, err := os.Open(path)
	if err != nil {
		logrus.Panicf("Failed to open %s:%s\n", path, err)
	}
	defer f.Close()

	rd := bufio.NewReader(f)
	for {
		//line, err := rd.ReadString('\n') //以'\n'为结束符读入一行
		line, _, err := rd.ReadLine()
		if err != nil || io.EOF == err {
			break
		}

		logrus.Printf("%s", string(line))

		info := strings.Split(string(line), " ")
		if len(info) < 3 {
			continue
		}

		Id, _ := strconv.Atoi(info[0])
		_, ok := Map.Load(Id)
		if ok {
			continue
		}else {
			Map.Store(Id, Id)
		}

		//log.Printf("miner id: %d", Id)
		pId, _ := peer.Decode(info[1])
		//log.Printf("peer id: %s", pId)
		addrsLen := len(info[2])
		addrsinfo := info[2][1:addrsLen-1]
		//logrus.Printf("addrsinfo:%s", addrsinfo)
		adds := strings.Split(addrsinfo, ",")
		//logrus.Printf("adds:%s", adds)
		var addrs []string
		for _, v := range adds {
			addr := v[1:len(v)-1]
			addrs = append(addrs, addr)
		}
		//logrus.Printf("addrs:%s", addrs)
		add := &AddInfo{Id, pId, addrsToMaddrs(addrs), addrs}
		addlist = append(addlist, add)
	}

	//data, err := ioutil.ReadFile(path)
	//if err != nil {
	//	logrus.Panicf("Failed to read %s:%s\n", path, err)
	//}
	//
	//dnlist := [] dnInfo{}
	//err = json.Unmarshal(data, dnlist)
	//if err != nil {
	//	logrus.Panicf("Failed to unmarshal %s:%s\n", path, err)
	//}

	//for _, v := range dnlist {
	//	pId, _ := peer.Decode(v.NodeID)
	//	add := &AddInfo{pId, addrsToMaddrs(v.IP)}
	//	addlist = append(addlist, add)
	//}

	return addlist
}

type DnList struct {
	Sdnlist []*AddInfo
	Ddnlist []*AddInfo
	L sync.Mutex
}

func (dl *DnList) UpdateList() {
	dl.L.Lock()
	defer dl.L.Unlock()

	path := os.Getenv("SDNLIST")
	if path == "" {
		path = GetCurrentPath() + "conf/sdnlist.properties"
	}
	dl.Sdnlist = GetDnlist(path)

	path = os.Getenv("DDNLIST")
	if path == "" {
		path = GetCurrentPath() + "conf/ddnlist.properties"
	}
	dl.Ddnlist = GetDnlist(path)
}

func (dl *DnList) IsAvalible() bool {
	dl.L.Lock()
	defer dl.L.Unlock()

	return len(dl.Ddnlist) > 0 && len(dl.Sdnlist) > 0
}
