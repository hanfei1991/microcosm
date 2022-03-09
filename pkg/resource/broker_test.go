package resource

import (
	"context"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProxyConcurrent(t *testing.T) {
	ctx := context.Background()
	testId := "TestProxyConcurrent"
	p, err := DefaultBroker.NewProxyForWorker(ctx, testId)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			p.ID()
			_, err := p.CreateFile(ctx, "test1"+strconv.Itoa(i))
			require.NoError(t, err)
			p.ID()
			_, err = p.CreateFile(ctx, "test2"+strconv.Itoa(i))
			require.NoError(t, err)
		}(i)
	}
	wg.Wait()

	err = os.RemoveAll("./resources/TestProxyConcurrent")
	require.NoError(t, err)
}
