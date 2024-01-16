package test

import (
	"testing"

	"github.com/csci1270-fall-2023/dbms-projects-handout/pkg/list"
)

func TestSample(t *testing.T) {
	l := list.NewList()
	if l.PeekHead() != nil || l.PeekTail() != nil {
		t.Fatal("bad list initialization")
	}
}
func TestListPushHead(t *testing.T) {
	l := list.NewList()
	l.PushHead("ABC")
	if l.PeekHead() != l.PeekTail() || l.PeekHead().GetKey().(string) != "ABC" {
		t.Fatal("bad list PushHead")
	}
	l.PushHead(123)
	if l.PeekHead().GetKey().(int) != 123 || l.PeekTail().GetKey().(string) != "ABC" {
		t.Fatal("bad list PushHead")
	}
}

func TestListPushTail(t *testing.T) {
	l := list.NewList()
	l.PushTail("ABC")
	if l.PeekHead() != l.PeekTail() || l.PeekHead().GetKey().(string) != "ABC" {
		t.Fatal("bad list PushHead")
	}
	l.PushTail(123)
	if l.PeekTail().GetKey().(int) != 123 || l.PeekHead().GetKey().(string) != "ABC" {
		t.Fatal("bad list PushHead")
	}
}

func TestPopNewTail(t *testing.T) {
	l := list.NewList()
	l.PushTail("ABC")
	l.PeekHead().PopSelf()
	if l.PeekHead() != nil || l.PeekTail() != nil {
		t.Fatal("bad pop")
	}
}

func TestListFind(t *testing.T) {
	l := list.NewList()
	l.PushHead(123)

	if l.Find(func(f *list.Link) bool { return true }).GetKey().(int) != 123 {
		t.Fatal("bad list Find!")
	}

}
