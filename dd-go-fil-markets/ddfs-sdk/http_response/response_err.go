//DD add
package http_response

import (
	"encoding/json"
	"fmt"
)

type ResponseErr struct {
	ResponseCode
	Data string `json:"data"`
}

func (resErr *ResponseErr) Error() string {
	info := fmt.Sprintf("code:%d msg:%v", resErr.Code, resErr.Msg)
	if resErr.Data == "" {
		return info
	}
	return fmt.Sprintf("%s Err:%v", info, resErr.Data)
}

func UnmarshalResponseErr(info []byte) (error, bool) {
	var resErr ResponseErr
	err := json.Unmarshal(info, &resErr)
	if err != nil {
		return nil, false
	}
	if IsOk(resErr.Code) {
		return nil, false
	}
	return &resErr, true
}

func IsResponseErr(err error) bool {
	_, ok := err.(*ResponseErr)
	return ok
}

func Is(err error, resCodes ...ResponseCode) bool {
	resErr, ok := err.(*ResponseErr)
	if !ok {
		return false
	}
	for _, resCode := range resCodes {
		if resErr.Code == resCode.Code {
			return true
		}
	}
	return false
}

func IsUnexpected(err error) bool {
	return Is(err, CurdCreatFail, CurdUpdateFail, CurdDeleteFail, CurdSelectFail, DBDataException)
}

var _ error = (*ResponseErr)(nil)

type Res500Err struct {
	StatusCode int
	Msg        string
	Url        string
}

func New500Err(statusCode int, statInfo string, url fmt.Stringer) error {
	return &Res500Err{
		StatusCode: statusCode,
		Msg:        statInfo,
		Url:        url.String(),
	}
}

func (resErr *Res500Err) Error() string {
	return fmt.Sprintf("%v:%v access:%v", resErr.Msg, resErr.StatusCode, resErr.Url)
}

var _ error = (*Res500Err)(nil)

func Is500Err(err error) bool {
	_, ok := err.(*Res500Err)
	if !ok {
		return false
	}
	return true
}
