package networks

import (
	"blockEmulator/params"
	"bytes"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"math/rand"

	"golang.org/x/time/rate"
)

// IsTransientNetErr returns true for TCP pool lifecycle errors that don't
// need to be logged (forcibly closed, connection reset, broken pipe).
func IsTransientNetErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "forcibly closed") ||
		strings.Contains(s, "connection reset") ||
		strings.Contains(s, "broken pipe") ||
		strings.Contains(s, "actively refused") ||
		strings.Contains(s, "connection refused")
}

var connMaplock sync.Mutex
var connectionPool = make(map[string]net.Conn, 0)

// network params.
var randomDelayGenerator *rand.Rand
var rateLimiterDownload *rate.Limiter
var rateLimiterUpload *rate.Limiter

// Define the latency, jitter and bandwidth here.
// Init tools.
func InitNetworkTools() {
	// avoid wrong params.
	if params.Delay < 0 {
		params.Delay = 0
	}
	if params.JitterRange < 0 {
		params.JitterRange = 0
	}
	if params.Bandwidth < 0 {
		params.Bandwidth = 0x7fffffff
	}

	// generate the random seed.
	randomDelayGenerator = rand.New(rand.NewSource(time.Now().UnixMicro()))
	// Limit the download rate
	rateLimiterDownload = rate.NewLimiter(rate.Limit(params.Bandwidth), params.Bandwidth)
	// Limit the upload rate
	rateLimiterUpload = rate.NewLimiter(rate.Limit(params.Bandwidth), params.Bandwidth)
}

func TcpDial(context []byte, addr string) {
	go func() {
		// simulate the delay
		thisDelay := params.Delay
		if params.JitterRange != 0 {
			thisDelay = randomDelayGenerator.Intn(params.JitterRange) - params.JitterRange/2 + params.Delay
		}
		time.Sleep(time.Millisecond * time.Duration(thisDelay))

		connMaplock.Lock()
		conn, ok := connectionPool[addr]
		connMaplock.Unlock()

		if !ok {
			var err error
			// Retry initial connection with backoff to handle startup race
			const maxRetries = 5
			for attempt := 0; attempt < maxRetries; attempt++ {
				conn, err = net.Dial("tcp", addr)
				if err == nil {
					break
				}
				if attempt < maxRetries-1 {
					time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
				}
			}
			if err != nil {
				if !IsTransientNetErr(err) {
					log.Println("Connect error", err)
				}
				return
			}
			connMaplock.Lock()
			connectionPool[addr] = conn
			connMaplock.Unlock()
		}

		data := append(context, '\n')
		_, err := writeToConnWithErr(data, conn, rateLimiterUpload)
		if err != nil {
			// connection stale, remove and retry
			connMaplock.Lock()
			if connectionPool[addr] == conn {
				delete(connectionPool, addr)
			}
			connMaplock.Unlock()
			conn.Close()

			var newConn net.Conn
			var dialErr error
			for ra := 0; ra < 3; ra++ {
				newConn, dialErr = net.Dial("tcp", addr)
				if dialErr == nil {
					break
				}
				if ra < 2 {
					time.Sleep(time.Duration(ra+1) * 500 * time.Millisecond)
				}
			}
			if dialErr != nil {
				if !IsTransientNetErr(dialErr) {
					log.Println("Reconnect error", dialErr)
				}
				return
			}
			connMaplock.Lock()
			connectionPool[addr] = newConn
			connMaplock.Unlock()

			_, retryErr := writeToConnWithErr(data, newConn, rateLimiterUpload)
			if retryErr != nil {
				if !IsTransientNetErr(retryErr) {
					log.Println("Write error after reconnect", retryErr)
				}
				connMaplock.Lock()
				if connectionPool[addr] == newConn {
					delete(connectionPool, addr)
				}
				connMaplock.Unlock()
				newConn.Close()
			}
		}
	}()
}

// Broadcast sends a message to multiple receivers, excluding the sender.
func Broadcast(sender string, receivers []string, msg []byte) {
	for _, ip := range receivers {
		if ip == sender {
			continue
		}
		go TcpDial(msg, ip)
	}
}

// CloseAllConnInPool closes all connections in the connection pool.
func CloseAllConnInPool() {
	connMaplock.Lock()
	defer connMaplock.Unlock()

	for _, conn := range connectionPool {
		conn.Close()
	}
	connectionPool = make(map[string]net.Conn) // Reset the pool
}

// ReadFromConn reads data from a connection.
func ReadFromConn(addr string) {
	conn := connectionPool[addr]

	// new a conn reader
	connReader := NewConnReader(conn, rateLimiterDownload)

	buffer := make([]byte, 1024)
	var messageBuffer bytes.Buffer

	for {
		n, err := connReader.Read(buffer)
		if err != nil {
			if err != io.EOF {
				log.Println("Read error for address", addr, ":", err)
			}
			break
		}

		// add message to buffer
		messageBuffer.Write(buffer[:n])

		// handle the full message
		for {
			message, err := readMessage(&messageBuffer)
			if err == io.ErrShortBuffer {
				// continue to load if buffer is short
				break
			} else if err == nil {
				// log the full message
				log.Println("Received from", addr, ":", message)
			} else {
				// handle other errs
				log.Println("Error processing message for address", addr, ":", err)
				break
			}
		}
	}
}

func readMessage(buffer *bytes.Buffer) (string, error) {
	message, err := buffer.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return "", err
	}
	return string(message), nil
}
