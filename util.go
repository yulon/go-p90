package p90

import (
	"bytes"
	"encoding/binary"
	"net"
)

func makeData(others ...interface{}) []byte {
	buf := bytes.NewBuffer([]byte{})
	for _, other := range others {
		switch other.(type) {
		case nil:
		case []byte:
			buf.Write(other.([]byte))
		default:
			binary.Write(buf, binary.LittleEndian, other)
		}
	}
	return buf.Bytes()
}

func makePacket(h *header, others ...interface{}) []byte {
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, h)
	for _, other := range others {
		switch other.(type) {
		case nil:
		case []byte:
			//binary.Write(buf, binary.LittleEndian, uint16(len(other.([]byte))))
			buf.Write(other.([]byte))
		default:
			binary.Write(buf, binary.LittleEndian, other)
		}
	}
	return buf.Bytes()
}

var ipv4Localhost = net.ParseIP("127.0.0.1")

func NewLocalUDPAddr(port int, remoteUDPAddr *net.UDPAddr) *net.UDPAddr {
	localUDPAddr := &net.UDPAddr{
		Port: port,
	}
	if remoteUDPAddr.IP.Equal(ipv4Localhost) {
		localUDPAddr.IP = ipv4Localhost
	} else {
		localUDPAddr.IP = net.IPv4zero
	}
	return localUDPAddr
}

func ResolveAddr(network, addrStr string) (net.Addr, error) {
	if len(network) < 2 {
		return nil, net.UnknownNetworkError(network)
	}
	if network[2:] == "ip" {
		return net.ResolveIPAddr(network, addrStr)
	}

	if len(network) < 3 {
		return nil, net.UnknownNetworkError(network)
	}
	switch network[3:] {
	case "udp":
		return net.ResolveUDPAddr(network, addrStr)
	case "tcp":
		return net.ResolveTCPAddr(network, addrStr)
	}

	if len(network) < 4 {
		return nil, net.UnknownNetworkError(network)
	}
	return net.ResolveUnixAddr(network, addrStr)
}
