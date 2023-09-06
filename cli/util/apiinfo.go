package cliutil

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

var log = logging.Logger("cliutil")

var (
	infoWithToken = regexp.MustCompile("^[a-zA-Z0-9\\-_]+?\\.[a-zA-Z0-9\\-_]+?\\.([a-zA-Z0-9\\-_]+)?:.+$")
)

type APIInfo struct {
	Addr  string
	Token []byte
}

func ParseApiInfo(s string) APIInfo {
	var tok []byte

	if infoWithToken.Match([]byte(s)) {
		sp := strings.SplitN(s, ":", 2)
		tok = []byte(sp[0])
		s = sp[1]
	}

	return APIInfo{
		Addr:  s,
		Token: tok,
	}
}

func ParseApiInfoMulti(s string) []APIInfo {
	var apiInfos []APIInfo

	allAddrs := strings.SplitN(s, ",", -1)

	for _, addr := range allAddrs {
		apiInfos = append(apiInfos, ParseApiInfo(addr))
	}

	return apiInfos
}

func isURLAvailable(url string, timeout time.Duration) bool {
	parts := strings.Split(url, "/")
	if len(parts) < 5 {
		return false
	}

	ip := parts[2]
	port := parts[4]

	address := fmt.Sprintf("%s:%s", ip, port)

	// 发送HTTP GET请求
	conn, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		fmt.Println("无法连接到地址:", err)
		return false
	}
	defer conn.Close()

	// log.Infof("成功连接到URL: %+v", url)
	return true
}

func ParseApiInfoMultiOfSxx(s string) []APIInfo {
	var apiInfos []APIInfo

	allAddrs := strings.SplitN(s, ",", -1)

	for _, addr := range allAddrs {
		apiInfos = append(apiInfos, ParseApiInfo(addr))
	}

	timeout := 2 * time.Second
	for i := 0; i < len(apiInfos); i++ {
		if !isURLAvailable(apiInfos[i].Addr, timeout) {
			apiInfos = append(apiInfos[:i], apiInfos[i+1:]...)
			i-- // 因为删除元素后切片长度减少了，需要减少索引以继续遍历
		}
	}

	return apiInfos
}

func (a APIInfo) DialArgs(version string) (string, error) {
	ma, err := multiaddr.NewMultiaddr(a.Addr)
	if err == nil {
		_, addr, err := manet.DialArgs(ma)
		if err != nil {
			return "", err
		}

		return "ws://" + addr + "/rpc/" + version, nil
	}

	_, err = url.Parse(a.Addr)
	if err != nil {
		return "", err
	}
	return a.Addr + "/rpc/" + version, nil
}

func (a APIInfo) Host() (string, error) {
	ma, err := multiaddr.NewMultiaddr(a.Addr)
	if err == nil {
		_, addr, err := manet.DialArgs(ma)
		if err != nil {
			return "", err
		}

		return addr, nil
	}

	spec, err := url.Parse(a.Addr)
	if err != nil {
		return "", err
	}
	return spec.Host, nil
}

func (a APIInfo) AuthHeader() http.Header {
	if len(a.Token) != 0 {
		headers := http.Header{}
		headers.Add("Authorization", "Bearer "+string(a.Token))
		return headers
	}
	log.Warn("API Token not set and requested, capabilities might be limited.")
	return nil
}
