package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Merge overwrites the contents of dst
func Merge(dst, left, right []int) {
	var li, ri int
	for li+ri < len(dst) {
		if ri >= len(right) || (li < len(left) && left[li] < right[ri]) {
			dst[li+ri] = left[li]
			li++
		} else {
			dst[li+ri] = right[ri]
			ri++
		}
	}
}

func MergeSort(data []int) {
	if len(data) <= 1 {
		return
	}
	pivot := len(data) / 2

	left := append([]int{}, data[:pivot]...)
	right := append([]int{}, data[pivot:]...)

	MergeSort(left)
	MergeSort(right)

	Merge(data, left, right)
}

func OverlyConcurrentMergeSort(data []int) {
	if len(data) <= 1 {
		return
	}
	pivot := len(data) / 2

	left := append([]int{}, data[:pivot]...)
	right := append([]int{}, data[pivot:]...)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		OverlyConcurrentMergeSort(left)
		wg.Done()
	}()
	OverlyConcurrentMergeSort(right)
	wg.Wait()

	Merge(data, left, right)
}

func ConcurrentPivotMergeSort(data []int) {
	var activeWorkers int32
	var aux func([]int)
	aux = func(data []int) {
		if len(data) <= 1 {
			return
		}
		pivot := len(data) / 2

		left := append([]int{}, data[:pivot]...)
		right := append([]int{}, data[pivot:]...)

		if atomic.LoadInt32(&activeWorkers) < int32(runtime.NumCPU()) && atomic.CompareAndSwapInt32(&activeWorkers, activeWorkers, activeWorkers+1) {

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				aux(left)
				wg.Done()
			}()
			aux(right)
			wg.Wait()
		} else {
			MergeSort(left)
			MergeSort(right)
		}
		Merge(data, left, right)
	}
	aux(data)
}

func CheckOrder(data []int) bool {
	for i := range data[1:] {
		if data[i+1] < data[i] {
			return false
		}
	}
	return true
}

func Randomize(data []int) {
	for i := range data {
		data[i] = rand.Intn(1000)
	}
}

func TimingTest(name string, data []int, merger func([]int)) {
	Randomize(data)
	start := time.Now()
	merger(data)
	if !CheckOrder(data) {
		fmt.Println(name, "failed to correctly sort the data")
	}
	fmt.Printf("%s: %v\n", name, time.Now().Sub(start))
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	data := make([]int, 1e5)
	TimingTest("Normal", data, MergeSort)
	TimingTest("Overly concurrent", data, OverlyConcurrentMergeSort)
	TimingTest("Slightly concurrent", data, ConcurrentPivotMergeSort)
}
