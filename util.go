package main

import (
	"fmt"
	"math"
	"net"
	"net/rpc/jsonrpc"
)

func pow2(x int) uint32 {
	return uint32(math.Pow(2, float64(x)))
}

func inbetween(a uint32, inbtw uint32, b uint32) bool {
	if a < b {
		if inbtw > a && inbtw < b {
			return true
		}
		return false
	}

	if inbtw > a || inbtw < b {
		return true
	}
	return false
}

func inbetweenUpperClosed(a uint32, inbtw uint32, b uint32) bool {
	if a < b {
		if inbtw > a && inbtw <= b {
			return true
		}
		return false
	}

	if inbtw > a || inbtw <= b {
		return true
	}
	return false
}

func greaterthan(a uint32, b uint32, gt uint32) bool {
	if a < b {
		if gt > a && gt > b {
			return true
		}
		return false
	}
	if a > gt && b < gt {
		return true
	}
	return false
}

func GetAddress() string {
	interfaces, err := net.Interfaces()
	if err != nil {
		panic(err)
	}

	for _, interf := range interfaces {
		flags := interf.Flags

		// get only not loopback and up interfaces
		if flags&(net.FlagLoopback|flags&net.FlagUp) == net.FlagUp {
			addrs, err := interf.Addrs()
			if err != nil {
				panic(err)
			}
			for _, addr := range addrs {
				if ipnet, ok := addr.(*net.IPNet); ok {
					if ip4 := ipnet.IP.To4(); len(ip4) == net.IPv4len {
						return ip4.String()
					}
				}
			}
		}

	}
	return ""
}

func checkError(err error) bool {
	if err != nil {
		fmt.Println("Error ", err.Error())
		return true
	} else {
		return false
	}
}

func handleClient(conn net.Conn) {
	jsonrpc.ServeConn(conn)
}
