//CSC 469 Week 6 Assignment
//Ayan Patel and Nick Papadakis

package main

import (
	"fmt"
	"math/rand"
	"sort"
	"crypto/md5"
)

const numNodes = 5

type Ring struct {
	Nodes Nodes
}

type Nodes []*Node

type Node struct {
	Id string
	HashId int
}

func hashId(key string) []byte {
	return md5.Sum([]byte(key))
}

func NewRing() *Ring {
	return &Ring{Nodes : Nodes{}}
}

func NewNode(id string) *Node {
	return &Node{Id : id, HashId: hashId(id)}
}

func (r *Ring) AddNode(id string) {
	node := NewNode(id)
	r.Nodes = append(r.Nodes, node)
	sort.Sort(r.Nodes)
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
		return r.Nodes[i].HashId > hashId(key)
	}
	return sort.Search(r.Nodes.Len(), searchFn)
}

func (r *Ring) Get(key string) string {
	i := r.search(id)
	if i >= r.Nodes.Len() {
		i = 0
	}
	return r.Nodes[i].Id
}

func (r *Ring) Put(key string, value int) {

}

func main() {
	r := NewRing()
	r.AddNode("A")
	r.AddNode("B")
	r.AddNode("C")
	r.AddNode("D")
	r.AddNode("E")
	
}