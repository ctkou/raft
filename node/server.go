package node

import (
  "context"
)

type Server interface {
  Listen() (err error)
  Close()
  Receive(ctx context.Context, buffer []byte) (byteRead int64, clientAddress string, err error)
  Send(ctx context.Context, clientAddress string, buffer []byte) (byteWritten int64, err error)
}
