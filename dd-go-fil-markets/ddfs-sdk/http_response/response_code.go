//DD add
package http_response

import "encoding/json"

type ResponseInfo struct {
	ResponseCode
	Data json.RawMessage `json:"data"`
}

type ResponseCode struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

func newResponseCode(code int, msg string) ResponseCode {
	return ResponseCode{
		Code: code,
		Msg:  msg,
	}
}

var (
	OK    = newResponseCode(0, "成功")
	FAIL  = newResponseCode(-1, "失败")
	OK200 = newResponseCode(200, "成功")

	ValidatorParamsCheckFail = newResponseCode(-400100, "参数校验失败")

	// CURD 常用业务状态码
	CurdCreatFail  = newResponseCode(-400200, "新增失败")
	CurdUpdateFail = newResponseCode(-400201, "更新失败")
	CurdDeleteFail = newResponseCode(-400202, "删除失败")
	CurdSelectFail = newResponseCode(-400203, "查询失败")

	//数据库数据异常
	DBDataException = newResponseCode(-400300, "数据库数据异常")

	//invoke DCManager
	DCManagerSignFail = newResponseCode(-401100, "DC Manager签名失败")

	//业务异常
	NotEnoughVerifiedData     = newResponseCode(-401200, "数据不足")
	DataErrorNoInCurrCluster  = newResponseCode(-401201, "数据不在当前集群")
	DataStateNotExpected      = newResponseCode(-401202, "数据状态不在预期")
	FilePathNotExist          = newResponseCode(-401203, "文件路径不存在")
	FileNotExist              = newResponseCode(-401204, "文件不存在")
	InsufficientRemainingTime = newResponseCode(-401205, "剩余时间不足，请重试")
	UnkownLocalMethod         = newResponseCode(-401206, "未知本地方法名")
	ResourceConflict          = newResponseCode(-401207, "资源冲突")
	DBDataNotFound            = newResponseCode(-401208, "数据不存在")
)

func IsOk(responseCode int) bool {
	switch responseCode {
	case OK.Code, OK200.Code:
		return true
	}
	return false
}
