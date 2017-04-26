package main

import (
	"hash"
	"hash/fnv"
	"math"
)

type Configuration struct {
	HashFunc func() hash.Hash32
	m        int
	r        int // Size of successor list.
	k        int // # of redundant successor nodes for data.
}

// ditch sha1 , use fnv, theres no need to store instance of hashfuc also.
//func (t *Configuration) getKey(ip string, port string) uint32 {
//	return t.getKey(ip + ":" + port)
//}

func (t *Configuration) getKey(nId *NodeIdentifier) uint32 {
	return t.getKey1(string(nId.getAddress()))
}

func (t *Configuration) getKey1(dataKey string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(dataKey))

	return h.Sum32() % uint32(math.Pow(2, float64(t.m)))
}

// Returns the default Ring configuration
func DefaultConfiguration() Configuration {
	return Configuration{fnv.New32a, 8, 3, 2}
}
