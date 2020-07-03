package p90

import (
	"fmt"
	"testing"
)

func TestConn(*testing.T) {
	pr, err := Listen("localhost:8828")
	if err != nil {
		panic(err)
	}
	go func() {
		con, err := pr.AcceptP90()
		if err != nil {
			panic(err)
		}

		buf := make([]byte, 2048)
		n, err := con.ReadPacket(buf)
		if err != nil {
			panic(err)
		}
		fmt.Println("server read packet:", string(buf[:n]))

		fmt.Println("server write packet: 246")
		_, err = con.WritePacket([]byte("246"))
		if err != nil {
			panic(err)
		}

		n, err = con.Read(buf)
		if err != nil {
			panic(err)
		}
		fmt.Println("server read:", string(buf[:n]))

		fmt.Println("server write: 000")
		_, err = con.Write([]byte("000"))
		if err != nil {
			panic(err)
		}

		fmt.Println("server close once:", con.Close())
		fmt.Println("server close twice:", con.Close())
	}()

	con, err := Dial("127.0.0.1:8828")
	if err != nil {
		panic(err)
	}

	fmt.Println("client write packet: 123")
	_, err = con.WritePacket([]byte("123"))
	if err != nil {
		panic(err)
	}

	buf := make([]byte, 2048)
	n, err := con.ReadPacket(buf)
	if err != nil {
		panic(err)
	}
	fmt.Println("client read packet:", string(buf[:n]))

	fmt.Println("client write: 111")
	_, err = con.Write([]byte("111"))
	if err != nil {
		panic(err)
	}

	n, err = con.Read(buf)
	if err != nil {
		panic(err)
	}
	fmt.Println("client read:", string(buf[:n]))

	fmt.Println("client close once:", con.Close())
	fmt.Println("client close twice:", con.Close())
}
