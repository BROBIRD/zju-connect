package gvisor

import (
	"bytes"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	clientpkg "github.com/mythologyli/zju-connect/client"
	"github.com/mythologyli/zju-connect/client/easyconnect"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
)

var joinedPacketSink []byte

func TestJoinPacketSlicesPreservesContent(t *testing.T) {
	slices := [][]byte{{0x45, 0x00}, {}, {0x12, 0x34, 0x56}, {0x78}}
	want := []byte{0x45, 0x00, 0x12, 0x34, 0x56, 0x78}
	if got := joinPacketSlices(slices); !bytes.Equal(got, want) {
		t.Fatalf("joinPacketSlices() = % X, want % X", got, want)
	}
}

func TestInboundPacketBufferPreservesReadBytes(t *testing.T) {
	buf := []byte{0x45, 0x00, 0x12, 0x34, 0x00, 0x00}
	const bytesRead = 4
	packet := makeInboundPacketBuffer(buf, bytesRead)
	defer packet.DecRef()
	got := joinPacketSlices(packet.AsSlices())
	if !bytes.Equal(got[:bytesRead], buf[:bytesRead]) {
		t.Fatalf("packet prefix = % X, want % X", got[:bytesRead], buf[:bytesRead])
	}
}

func TestInboundPacketBufferExcludesUnreadCapacity(t *testing.T) {
	buf := []byte{0x45, 0x00, 0x12, 0x34, 0xde, 0xad}
	const bytesRead = 4
	packet := makeInboundPacketBuffer(buf, bytesRead)
	defer packet.DecRef()

	got := joinPacketSlices(packet.AsSlices())
	if !bytes.Equal(got, buf[:bytesRead]) {
		t.Fatalf("packet = % X, want only read bytes % X", got, buf[:bytesRead])
	}
}

type stubL3Conn struct {
	writeErr error

	closeOnce sync.Once
	closed    chan struct{}
}

func newStubL3Conn(writeErr error) *stubL3Conn {
	return &stubL3Conn{writeErr: writeErr, closed: make(chan struct{})}
}

func (c *stubL3Conn) Read(buf []byte) (int, error) {
	<-c.closed
	return 0, net.ErrClosed
}

func (c *stubL3Conn) Write(buf []byte) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	return len(buf), nil
}

func (c *stubL3Conn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

type stubClient struct {
	clientpkg.Client

	l3Conn *stubL3Conn
	connCh chan *stubL3Conn
}

func newStubClient(l3Conn *stubL3Conn) *stubClient {
	return &stubClient{l3Conn: l3Conn, connCh: make(chan *stubL3Conn, 1)}
}

func (c *stubClient) NewL3Conn() (io.ReadWriteCloser, error) {
	c.connCh <- c.l3Conn
	return c.l3Conn, nil
}

func TestCloseBeforeRunIsHarmlessAndIdempotent(t *testing.T) {
	s := &Stack{}
	s.endpoint = &Endpoint{client: newStubClient(nil), stack: s}

	s.Close()
	s.Close()

	if !s.isClosed() {
		t.Fatal("Close did not mark the stack closed")
	}
}

func TestRunReturnsAfterCloseWithoutPanicking(t *testing.T) {
	client := newStubClient(newStubL3Conn(nil))
	s := &Stack{}
	s.endpoint = &Endpoint{client: client, stack: s}

	runReturned := make(chan struct{})
	go func() {
		s.Run()
		close(runReturned)
	}()

	// Run hands the connection over synchronously before entering the read
	// loop; the buffered hand-off channel therefore marks that point.
	select {
	case <-client.connCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not create the L3 connection")
	}

	s.Close()

	select {
	case <-runReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after Close")
	}
}

func TestWritePacketsReportsShutdownToHandlerInsteadOfExiting(t *testing.T) {
	l3Conn := newStubL3Conn(easyconnect.ErrSangforShutdown)
	s := &Stack{}
	s.endpoint = &Endpoint{client: &stubClient{l3Conn: l3Conn}, stack: s, l3Conn: l3Conn}

	handlerErr := make(chan error, 1)
	s.SetShutdownHandler(func(err error) { handlerErr <- err })

	packet := makeInboundPacketBuffer([]byte{0x45, 0x00, 0x00, 0x14}, 4)
	defer packet.DecRef()
	list := stack.PacketBufferList{}
	list.PushBack(packet)

	if n, tcpipErr := s.endpoint.WritePackets(list); n != 1 || tcpipErr != nil {
		t.Fatalf("WritePackets() = %d, %v; want 1, nil", n, tcpipErr)
	}

	select {
	case err := <-handlerErr:
		if !errors.Is(err, easyconnect.ErrSangforShutdown) {
			t.Fatalf("shutdown handler received %v, want ErrSangforShutdown", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown handler was not invoked")
	}
}

func BenchmarkJoinPacketSlices(b *testing.B) {
	slices := [][]byte{make([]byte, 20), make([]byte, 20), make([]byte, 1160)}
	b.ReportAllocs()
	b.SetBytes(1200)
	for i := 0; i < b.N; i++ {
		joinedPacketSink = joinPacketSlices(slices)
	}
}
