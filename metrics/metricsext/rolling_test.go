package metricsext

import (
	"sync"
	"testing"
)

func BenchmarkRollingAggregation(b *testing.B) {
	var r RollingAggregation
	for i:=0;i<b.N;i++ {
		r.Observe(1)
	}
}

func BenchmarkRollingAggregation1000(b *testing.B) {
	var r RollingAggregation
	var wg sync.WaitGroup
	const numToDo = 1000
	wg.Add(numToDo)
	for i :=0;i<numToDo;i++ {
		go func() {
			defer wg.Done()
			for i:=0;i<b.N / numToDo;i++ {
				r.Observe(1)
			}
		}()
	}
	wg.Wait()
}