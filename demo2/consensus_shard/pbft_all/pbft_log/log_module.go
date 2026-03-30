package pbft_log

import (
	"blockEmulator/params"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
)

type PbftLog struct {
	Plog *log.Logger
}

// asyncWriter wraps any io.Writer with a buffered channel so that Write
// never blocks the caller.  If the internal buffer is full the message is
// silently dropped — we prefer losing a console line over dead-locking the
// PBFT message-dispatch loop (which holds tcpPoolLock).
type asyncWriter struct {
	ch chan []byte
}

func newAsyncWriter(w io.Writer) *asyncWriter {
	aw := &asyncWriter{ch: make(chan []byte, 4096)}
	go func() {
		for b := range aw.ch {
			w.Write(b) //nolint:errcheck
		}
	}()
	return aw
}

func (aw *asyncWriter) Write(p []byte) (n int, err error) {
	cp := make([]byte, len(p))
	copy(cp, p)
	select {
	case aw.ch <- cp:
	default: // buffer full — drop rather than block
	}
	return len(p), nil
}

func NewPbftLog(sid, nid uint64) *PbftLog {
	pfx := fmt.Sprintf("S%dN%d: ", sid, nid)

	// stdout writes go through an async non-blocking writer so that a full
	// Windows console pipe can never dead-lock the PBFT TCP receive loop.
	asyncStdout := newAsyncWriter(os.Stdout)

	dirpath := params.LogWrite_path + "/S" + strconv.Itoa(int(sid))
	err := os.MkdirAll(dirpath, os.ModePerm)
	if err != nil {
		log.Panic(err)
	}
	writer2, err := os.OpenFile(dirpath+"/N"+strconv.Itoa(int(nid))+".log", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		log.Panic(err)
	}
	pl := log.New(io.MultiWriter(asyncStdout, writer2), pfx, log.Lshortfile|log.Ldate|log.Ltime)
	fmt.Println()

	return &PbftLog{
		Plog: pl,
	}
}
