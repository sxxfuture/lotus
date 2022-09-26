//DD add
package api

import "io"

type RemoteFile interface {
	io.ReadCloser
	Size() (uint64, error)
}
