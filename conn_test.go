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

		data, err := con.Recv()
		if err != nil {
			panic(err)
		}
		fmt.Println("server recv:", string(data))

		fmt.Println("server send: 246")
		err = con.Send([]byte("246"))
		if err != nil {
			panic(err)
		}

		_, err = con.Read(data)
		if err != nil {
			panic(err)
		}
		fmt.Println("server read:", string(data))

		fmt.Println("server write: 000")
		_, err = con.Write([]byte("000"))
		if err != nil {
			panic(err)
		}
	}()

	con, err := Dial("127.0.0.1:8828")
	if err != nil {
		panic(err)
	}

	fmt.Println("client send: 123")
	err = con.Send([]byte("123"))
	if err != nil {
		panic(err)
	}

	data, err := con.Recv()
	if err != nil {
		panic(err)
	}
	fmt.Println("client recv:", string(data))

	fmt.Println("client write: 111")
	_, err = con.Write([]byte("111"))
	if err != nil {
		panic(err)
	}

	_, err = con.Read(data)
	if err != nil {
		panic(err)
	}
	fmt.Println("client read:", string(data))
}
