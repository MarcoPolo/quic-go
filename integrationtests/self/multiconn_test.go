package self_test

import (
	"net"
	"sync"
	"time"
)

type dataAndAddr struct {
	data []byte
	addr net.Addr
}

// multiConn is a hack to have a single packetConn that supports v4 and v6 on
// macOS. There is probably a better way of doing this (see sysconn oob).
type multiConn struct {
	conn1 *net.UDPConn
	conn2 *net.UDPConn

	mu     sync.Mutex
	buffer []dataAndAddr
	closed bool

	readCh chan dataAndAddr
	done   chan struct{}
	once   sync.Once
}

var _ net.PacketConn = (*multiConn)(nil)

func newMultiConn(conn1, conn2 *net.UDPConn) *multiConn {
	mc := &multiConn{
		conn1:  conn1,
		conn2:  conn2,
		readCh: make(chan dataAndAddr, 100),
		done:   make(chan struct{}),
	}
	mc.startReaders()
	return mc
}

func (mc *multiConn) startReaders() {
	go mc.readLoop(mc.conn1)
	go mc.readLoop(mc.conn2)
}

func (mc *multiConn) readLoop(conn *net.UDPConn) {
	buf := make([]byte, 1500)
	for {
		select {
		case <-mc.done:
			return
		default:
		}

		n, addr, err := conn.ReadFrom(buf)
		if err != nil {
			select {
			case <-mc.done:
				return
			default:
				continue
			}
		}

		data := make([]byte, n)
		copy(data, buf[:n])

		select {
		case mc.readCh <- dataAndAddr{data: data, addr: addr}:
		case <-mc.done:
			return
		}
	}
}

func (mc *multiConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	mc.mu.Lock()
	if mc.closed {
		mc.mu.Unlock()
		return 0, nil, net.ErrClosed
	}

	if len(mc.buffer) > 0 {
		pkt := mc.buffer[0]
		mc.buffer = mc.buffer[1:]
		mc.mu.Unlock()

		n = copy(p, pkt.data)
		return n, pkt.addr, nil
	}
	mc.mu.Unlock()

	select {
	case pkt := <-mc.readCh:
		n = copy(p, pkt.data)
		return n, pkt.addr, nil
	case <-mc.done:
		return 0, nil, net.ErrClosed
	}
}

func (mc *multiConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if mc.closed {
		return 0, net.ErrClosed
	}

	udpAddr, ok := addr.(*net.UDPAddr)
	if !ok {
		return mc.conn1.WriteTo(p, addr)
	}

	conn1Addr := mc.conn1.LocalAddr().(*net.UDPAddr)

	if (udpAddr.IP.To4() != nil) == (conn1Addr.IP.To4() != nil) {
		return mc.conn1.WriteTo(p, addr)
	}

	return mc.conn2.WriteTo(p, addr)
}

func (mc *multiConn) Close() error {
	mc.once.Do(func() {
		mc.mu.Lock()
		mc.closed = true
		mc.mu.Unlock()
		close(mc.done)
	})

	err1 := mc.conn1.Close()
	err2 := mc.conn2.Close()

	if err1 != nil {
		return err1
	}
	return err2
}

func (mc *multiConn) LocalAddr() net.Addr {
	return mc.conn1.LocalAddr()
}

func (mc *multiConn) SetDeadline(t time.Time) error {
	err1 := mc.conn1.SetDeadline(t)
	err2 := mc.conn2.SetDeadline(t)

	if err1 != nil {
		return err1
	}
	return err2
}

func (mc *multiConn) SetReadDeadline(t time.Time) error {
	err1 := mc.conn1.SetReadDeadline(t)
	err2 := mc.conn2.SetReadDeadline(t)

	if err1 != nil {
		return err1
	}
	return err2
}

func (mc *multiConn) SetWriteDeadline(t time.Time) error {
	err1 := mc.conn1.SetWriteDeadline(t)
	err2 := mc.conn2.SetWriteDeadline(t)

	if err1 != nil {
		return err1
	}
	return err2
}
