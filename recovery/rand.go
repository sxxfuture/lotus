package recovery

import "math/rand"

func RandBytes(num uint64) ([]byte,error) {
	bz := make([]byte, num)
	if _,err := rand.Read(bz);err!=nil {
		return nil,err
	}

	return bz,nil
}
