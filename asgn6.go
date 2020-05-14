//CSC 469 Week 6 Assignment
//Ayan Patel and Nick Papadakis

package main

import (
	"fmt"
	"sort"
	"crypto/md5"
	"encoding/binary"
)

const numNodes = 5

type message struct {
	dest string
	key string
	value int
}

type Ring struct {
	Nodes Nodes
	messageChan chan message
}

type Nodes []*Node

type Node struct {
	Id string
	HashId uint64
}

func hashId(key string) uint64 {
	s := md5.Sum([]byte(key))
	h := binary.LittleEndian.Uint64(s[:])
	return h
}

func NewRing() *Ring {
	return &Ring{Nodes : Nodes{}, messageChan : make(chan message, 32)}
}

func NewNode(id string) *Node {
	return &Node{Id : id, HashId: hashId(id)}
}

func (r *Ring) AddNode(id string) {
	node := NewNode(id)
	r.Nodes = append(r.Nodes, node)
	sort.Sort(r.Nodes)
	go nodeRoutine(id, r.messageChan)
}

func (r *Ring) DeleteNode(id string) {
	i := r.search(id)
	if i >= r.Nodes.Len() || r.Nodes[i].Id != id {
		fmt.Println("node not found")
	}
	r.Nodes = append(r.Nodes[:i], r.Nodes[i+1:]...)
}

func (n Nodes) Len() int {
	return len(n)
}

func (n Nodes) Less(i, j int) bool {
	return n[i].HashId < n[j].HashId
}

func (n Nodes) Swap(i, j int) {
	n[i], n[j] = n[j], n[i]
}

func (r *Ring) search(id string) int {
	searchFn := func(i int) bool {
		return r.Nodes[i].HashId >= hashId(id)
	}
	return sort.Search(r.Nodes.Len(), searchFn)
}

func (r *Ring) Get(key string) string {
	i := r.search(key)
	if i >= r.Nodes.Len() {
		i = 0
	}
	return r.Nodes[i].Id
}

func (r *Ring) Put(key string, value int) {
	var m message
	m.dest = r.Get(key)
	m.key = key
	m.value = value
	r.messageChan <- m
}

func nodeRoutine(id string, messageChan chan message) {
	db := make(map[string]int)

	for {
		select {
		case recvMessage := <- messageChan:
			if recvMessage.dest == id {
				db[recvMessage.key] = recvMessage.value
			} else {
				messageChan <- recvMessage
			}
		default:
		}
	}
}

func main() {
	r := NewRing()
	r.AddNode("A")
	r.AddNode("B")
	r.AddNode("C")
	r.AddNode("D")
	r.AddNode("E")

	r.Put("Maria", 10)

	n_id := r.Get("Maria")
	fmt.Println("Maria at node", n_id)

	r.DeleteNode("E")

	n_id = r.Get("Maria")
	fmt.Println("Maria at node", n_id)
}