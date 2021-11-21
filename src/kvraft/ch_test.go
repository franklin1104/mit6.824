package kvraft

import "testing"
import "fmt"

func TestChannel(t *testing.T) {
	emptyString := ""
	tempCh := make(chan interface{})
	select {
	case tempCh <- emptyString:
		fmt.Printf("empty")
	default:
		fmt.Printf("default")
	}
}
