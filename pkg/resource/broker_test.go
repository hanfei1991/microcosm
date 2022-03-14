package resource

import (
	"context"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProxyConcurrent(t *testing.T) {
	ctx := context.Background()
	testID := "TestProxyConcurrent"
	p, err := MockBroker.NewProxyForWorker(ctx, testID)
	require.NoError(t, err)

	require.Equal(t, MockBroker.AllocatedIDs(), []string{testID})

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

	err = os.RemoveAll("./resources")
	require.NoError(t, err)
}

func TestCollectLocalAndRemove(t *testing.T) {
	defer func() {
		err := os.RemoveAll("./resources")
		require.NoError(t, err)
	}()
	err := os.MkdirAll("./resources/id-1", 0777)
	require.NoError(t, err)
	err = os.MkdirAll("./resources/id-2", 0777)
	require.NoError(t, err)

	allocated := MockBroker.AllocatedIDs()
	sort.Strings(allocated)
	require.Equal(t, []string{"id-1", "id-2"}, allocated)

	MockBroker.Remove("id-1")
	allocated = MockBroker.AllocatedIDs()
	require.Equal(t, []string{"id-2"}, allocated)
	// should not exist
	_, err = os.Stat("./resources/id-1")
	require.EqualError(t, err, "stat ./resources/id-1: no such file or directory")
}
