//CSC 469 Week 6 Assignment
//Ayan Patel and Nick Papadakis

package main

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sort"
)

const tokensPerNode = 4

type message struct {
	dest string
	key string
	value int
}

type Ring struct {
	tokens TokenList
	messageChan chan message
}

type Token struct {
	id uint64
	node *Node
}

type TokenList []Token

type Node struct {
	id string
}

func (r *Ring) printTokens() {
	for i := 0; i < len(r.tokens); i++ {
		fmt.Println(r.tokens[i].id, r.tokens[i].node.id)
	}
}

func hashId(key string) uint64 {
	s := md5.Sum([]byte(key))
	h := binary.LittleEndian.Uint64(s[:])
	return h
}

func NewRing() *Ring {
	return &Ring{tokens : []Token{},
		messageChan : make(chan message, 32)}
}

func NewNode(id string) *Node {
	n := new(Node)
	n.id = id
	return n
}

func (r *Ring) AddNode(id string) {
	node := NewNode(id)
	for i := 0; i < tokensPerNode; i++ {
		var t Token
		t.id = rand.Uint64()
		t.node = node
		r.tokens = append(r.tokens, t)
	}
	sort.Sort(r.tokens)
	go nodeRoutine(id, r.messageChan)
}

func (r *Ring) DeleteNode(id string) {
	for i := 0; i < len(r.tokens); i++ {
		if r.tokens[i].node.id == id {
			r.tokens = append(r.tokens[:i], r.tokens[i+1:]...)
			i-- //gotta check this one again since we just modified the list
		}
	}
}

func (t TokenList) Len() int {
	return len(t)
}

func (t TokenList) Less(i, j int) bool {
	return t[i].id < t[j].id
}

func (t TokenList) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func (r *Ring) search(id string) int {
	searchFn := func(i int) bool {
		return r.tokens[i].id >= hashId(id)
	}
	return sort.Search(r.tokens.Len(), searchFn)
}

func (r *Ring) Get(key string) string {
	i := r.search(key)
	if i >= r.tokens.Len() {
		i = 0
	}
	return r.tokens[i].node.id
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

func (r *Ring) PrintLocation(key string) {
	fmt.Println(key, "is on server", r.Get(key))
}

func main() {
	r := NewRing()
	r.AddNode("A")
	r.AddNode("B")
	r.AddNode("C")
	r.AddNode("D")
	r.AddNode("E")

	r.Put("Maria", 100)
	r.Put("John", 20)
	r.Put("Anna", 40)
	r.Put("Tim", 100)
	r.Put("Alex", 10)

	r.PrintLocation("Maria")
	r.PrintLocation("John")
	r.PrintLocation("Anna")
	r.PrintLocation("Tim")
	r.PrintLocation("Alex")

	fmt.Println("Deleting C")
	r.DeleteNode("C")

	r.PrintLocation("Maria")
	r.PrintLocation("John")
	r.PrintLocation("Anna")
	r.PrintLocation("Tim")
	r.PrintLocation("Alex")
}