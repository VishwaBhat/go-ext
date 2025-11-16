package conc

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestRun(t *testing.T) {

	elements := []string{
		"token=hello_world",
		"abc=xyz test",
		"test",
	}
	pattern := regexp.MustCompile(`.+=(\w+)\b`)

	runner := NewRunner[string, string](WithTimeout(time.Second))

	mapFunc := func(val string) (string, error) {
		matches := pattern.FindStringSubmatch(val)
		if matches == nil {
			return "", nil
		}
		return matches[len(matches)-1], nil
	}

	filterEmpty := func(val string) bool {
		return val == ""
	}

	results, err := runner.Map(elements, mapFunc, filterEmpty)
	require.NoError(t, err)
	fmt.Println(results)
}

func TestPipeline(t *testing.T) {
	elements := []string{
		"token=hello_world",
		"abc=xyz test",
		"test",
	}
	pattern := regexp.MustCompile(`.+=(\w+)\b`)

	r := NewRunner[string, string]()

	transform := func(input string, val any) (string, error) {
		return val.(string), nil
	}

	lowerCased := func(input string, val any) (any, error) {
		return strings.ToLower(input), nil
	}
	regexMatch := func(input string, lowerCased any) (any, error) {
		matches := pattern.FindStringSubmatch(lowerCased.(string))
		if matches == nil {
			return "", nil
		}
		return matches[len(matches)-1], nil
	}

	results, err := r.Pipeline(context.Background(),
		elements,
		transform,
		lowerCased,
		regexMatch,
	)

	require.NoError(t, err)
	fmt.Println(results)
}

func Test(t *testing.T) {
	/*

		tasker := NewTasker[string,int]()
		tasker.SetTask(func(v string) (int, error) {
			return re.Match(v)
		})

		tasker.EnqueueInput("hello")
		tasker.EnqueueInput("world")


		vals, err := tasker.Wait()

	*/
}
