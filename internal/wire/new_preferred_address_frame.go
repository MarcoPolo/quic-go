package wire

import (
	"encoding/binary"
	"io"

	"github.com/quic-go/quic-go/internal/protocol"
	"github.com/quic-go/quic-go/quicvarint"
)

type NewPreferredAddressFrame struct {
	SequenceNumber uint64
	IP4            [4]byte
	IP4Port        uint16
	IP6            [16]byte
	IP6Port        uint16
}

func parseNewPreferredAddressFrame(b []byte, _ protocol.Version) (*NewPreferredAddressFrame, int, error) {
	startLen := len(b)

	seq, l, err := quicvarint.Parse(b)
	if err != nil {
		return nil, 0, replaceUnexpectedEOF(err)
	}
	b = b[l:]

	if len(b) < 4 {
		return nil, 0, io.EOF
	}

	frame := &NewPreferredAddressFrame{
		SequenceNumber: seq,
	}

	copy(frame.IP4[:], b[:4])
	b = b[4:]

	if len(b) < 2 {
		return nil, 0, io.EOF
	}
	frame.IP4Port = binary.BigEndian.Uint16(b[:2])
	b = b[2:]

	if len(b) < 16 {
		return nil, 0, io.EOF
	}
	copy(frame.IP6[:], b[:16])
	b = b[16:]

	if len(b) < 2 {
		return nil, 0, io.EOF
	}
	frame.IP6Port = binary.BigEndian.Uint16(b[:2])

	return frame, startLen - len(b) + 2, nil
}

func (f *NewPreferredAddressFrame) Append(b []byte, _ protocol.Version) ([]byte, error) {
	b = quicvarint.Append(b, uint64(FrameTypeNewPreferredAddress))
	b = quicvarint.Append(b, f.SequenceNumber)
	b = append(b, f.IP4[:]...)

	portBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(portBytes, f.IP4Port)
	b = append(b, portBytes...)

	b = append(b, f.IP6[:]...)

	binary.BigEndian.PutUint16(portBytes, f.IP6Port)
	b = append(b, portBytes...)

	return b, nil
}

func (f *NewPreferredAddressFrame) Length(_ protocol.Version) protocol.ByteCount {
	return protocol.ByteCount(quicvarint.Len(uint64(FrameTypeNewPreferredAddress)) +
		quicvarint.Len(f.SequenceNumber) + 4 + 2 + 16 + 2)
}
