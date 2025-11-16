package slices2

import (
	"fmt"
	"testing"
)

func TestDedupe(t *testing.T) {
	type A struct {
		a *string
		b int
	}
	vals := []A{
		{nil, 1}, {nil, 2},
	}
	deduped := Unique(vals, func(t1, t2 A) bool {
		return t1.a == t2.a && t1.b == t2.b
	})
	fmt.Println(deduped)
}
