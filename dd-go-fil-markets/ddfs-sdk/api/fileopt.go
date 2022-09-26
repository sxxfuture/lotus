//DD add
package api

import (
	"fmt"
	"github.com/filecoin-project/go-fil-markets/ddfs-sdk/http_response"
	logging "github.com/ipfs/go-log/v2"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var log = logging.Logger("ddfs-api")

type DDFileOpt struct {
	file string
	host string
}

const FetchFileUrl = "/api/v0/file_opt/fetch"

const RevertFileStateUrl = "/api/v0/file_opt/revert"

const ConfirmFileUrl = "/api/v0/file_opt/confirm"

func (fo *DDFileOpt) FetchWithConfirm() (io.ReadCloser, uint64, error) {
	return fo.fetch(false)
}

func (fo *DDFileOpt) Fetch() (io.ReadCloser, uint64, error) {
	return fo.fetch(true)
}

func (fo *DDFileOpt) fetch(offerConfirmation bool) (io.ReadCloser, uint64, error) {
	fetchUrl, err := url.Parse(fo.host)
	if err != nil {
		log.Errorf("[DD] parse url failed url: %v,err: %v", fo.host, err)
		return nil, 0, err
	}
	fetchUrl, err = fetchUrl.Parse(FetchFileUrl)
	if err != nil {
		log.Errorf("[DD] parse url failed url: %v,err: %v", FetchFileUrl, err)
		return nil, 0, err
	}
	v := url.Values{}
	v.Add("file", fo.file)
	v.Add("offer_confirmation", strconv.FormatBool(offerConfirmation))
	fetchUrl.RawQuery = v.Encode()

	req, err := http.NewRequest(http.MethodGet, fetchUrl.String(), nil)
	if err != nil {
		log.Errorf("[DD] NewRequest failed err: %v", err)
		return nil, 0, err
	}
	req.Close = true
	req.Header = http.Header{}
	cli := http.Client{}
	cli.Timeout = time.Hour * 24

	resp, err := cli.Do(req)
	if err != nil {
		log.Errorf("[DD] request http failed err: %v", err)
		return nil, 0, err
	}

	if resp.StatusCode >= 500 {
		err = http_response.New500Err(resp.StatusCode, resp.Status, fetchUrl)
		log.Errorf("[DD] err: %v", err)
		return nil, 0, err
	}

	if 400 <= resp.StatusCode && resp.StatusCode < 500 {
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Errorf("[DD] %v:%v access:%v, err: %v", resp.Status, resp.StatusCode, fetchUrl, err)
			return nil, 0, fmt.Errorf("%v:%v access:%v, err: %v", resp.Status, resp.StatusCode, fetchUrl, err)
		}
		if err, ok := http_response.UnmarshalResponseErr(body); ok {
			return nil, 0, err
		}
		log.Errorf("[DD] %v:%v access:%v, body: %v", resp.Status, resp.StatusCode, fetchUrl, string(body))
		return nil, 0, fmt.Errorf("%v:%v access:%v, body: %v", resp.Status, resp.StatusCode, fetchUrl, string(body))
	}

	if resp.ContentLength < 0 {
		return nil, 0, fmt.Errorf("http can't get known ContentLength,access:%v", fetchUrl)
	}

	return resp.Body, uint64(resp.ContentLength), nil
}

func (fo *DDFileOpt) Confirm(key string) error {
	revertUrl, err := url.Parse(fo.host)
	if err != nil {
		log.Errorf("[DD] parse url failed url: %v,err: %v", fo.host, err)
		return err
	}
	revertUrl, err = revertUrl.Parse(ConfirmFileUrl)
	if err != nil {
		log.Errorf("[DD] parse url failed url: %v,err: %v", ConfirmFileUrl, err)
		return err
	}
	v := url.Values{}
	v.Add("file", fo.file)
	v.Add("key", key)
	revertUrl.RawQuery = v.Encode()

	req, err := http.NewRequest(http.MethodPut, revertUrl.String(), nil)
	if err != nil {
		log.Errorf("[DD] NewRequest failed err: %v", err)
		return err
	}

	req.Close = true
	req.Header = http.Header{}

	cli := http.Client{}
	cli.Timeout = time.Second * 30
	resp, err := cli.Do(req)
	if err != nil {
		log.Errorf("[DD] request http failed err: %v", err)
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode >= 500 {
		err = http_response.New500Err(resp.StatusCode, resp.Status, revertUrl)
		log.Errorf("[DD] err: %v", err)
		return err
	}

	if 400 <= resp.StatusCode && resp.StatusCode < 500 {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Errorf("[DD] %v:%v access:%v, err: %v", resp.Status, resp.StatusCode, revertUrl, err)
			return fmt.Errorf("%v:%v access:%v, err: %v", resp.Status, resp.StatusCode, revertUrl, err)
		}
		if err, ok := http_response.UnmarshalResponseErr(body); ok {
			return err
		}
		log.Errorf("[DD] %v:%v access:%v, body: %v", resp.Status, resp.StatusCode, revertUrl, string(body))
		return fmt.Errorf("%v:%v access:%v, body: %v", resp.Status, resp.StatusCode, revertUrl, string(body))
	}
	return nil
}

func (fo *DDFileOpt) Revert() error {
	revertUrl, err := url.Parse(fo.host)
	if err != nil {
		log.Errorf("[DD] parse url failed url: %v,err: %v", fo.host, err)
		return err
	}
	revertUrl, err = revertUrl.Parse(RevertFileStateUrl)
	if err != nil {
		log.Errorf("[DD] parse url failed url: %v,err: %v", RevertFileStateUrl, err)
		return err
	}
	v := url.Values{}
	v.Add("file", fo.file)
	revertUrl.RawQuery = v.Encode()

	req, err := http.NewRequest(http.MethodPut, revertUrl.String(), nil)
	if err != nil {
		log.Errorf("[DD] NewRequest failed err: %v", err)
		return err
	}

	req.Close = true
	req.Header = http.Header{}

	cli := http.Client{}
	cli.Timeout = time.Second * 30
	resp, err := cli.Do(req)
	if err != nil {
		log.Errorf("[DD] request http failed err: %v", err)
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode >= 500 {
		err = http_response.New500Err(resp.StatusCode, resp.Status, revertUrl)
		log.Errorf("[DD] err: %v", err)
		return err
	}

	if 400 <= resp.StatusCode && resp.StatusCode < 500 {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Errorf("[DD] %v:%v access:%v, err: %v", resp.Status, resp.StatusCode, revertUrl, err)
			return fmt.Errorf("%v:%v access:%v, err: %v", resp.Status, resp.StatusCode, revertUrl, err)
		}
		if err, ok := http_response.UnmarshalResponseErr(body); ok {
			return err
		}
		log.Errorf("[DD] %v:%v access:%v, body: %v", resp.Status, resp.StatusCode, revertUrl, string(body))
		return fmt.Errorf("%v:%v access:%v, body: %v", resp.Status, resp.StatusCode, revertUrl, string(body))
	}
	return nil
}

func NewFileOpt(remoteFileUrl string) (*DDFileOpt, error) {
	ss := strings.Split(remoteFileUrl, "|")
	if len(ss) != 2 {
		return nil, fmt.Errorf("unknown remoteFileUrl: %v", remoteFileUrl)
	}

	return &DDFileOpt{
		host: ss[0],
		file: ss[1],
	}, nil
}
