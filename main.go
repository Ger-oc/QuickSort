package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

func readNumbersFromCSV(filename string) ([]int, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	numbers := make([]int, 0, len(records))
	for _, record := range records {
		for _, value := range record {
			num, err := strconv.Atoi(value)
			if err != nil {
				return nil, err
			}
			numbers = append(numbers, num)
		}
	}

	return numbers, nil
}

func writeNumbersToCSV(filename string, numbers []int) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, num := range numbers {
		err := writer.Write([]string{strconv.Itoa(num)})
		if err != nil {
			return err
		}
	}

	return nil
}

func partition(arr []int, low, high int) int {
	pivot := arr[high]
	i := low - 1

	for j := low; j < high; j++ {
		if arr[j] < pivot {
			i++
			arr[i], arr[j] = arr[j], arr[i]
		}
	}

	arr[i+1], arr[high] = arr[high], arr[i+1]
	return i + 1
}

func quicksort(arr []int, low, high int, wg *sync.WaitGroup) {
	defer wg.Done()

	if low < high {
		pivot := partition(arr, low, high)

		var wgLeft, wgRight sync.WaitGroup
		wgLeft.Add(1)
		wgRight.Add(1)

		go func() {
			quicksort(arr, low, pivot-1, &wgLeft)
		}()

		go func() {
			quicksort(arr, pivot+1, high, &wgRight)
		}()

		wgLeft.Wait()
		wgRight.Wait()
	}
}

func runConcurrentQuicksort(arr []int) {
	var wg sync.WaitGroup
	wg.Add(1)
	quicksort(arr, 0, len(arr)-1, &wg)
	wg.Wait()
}

func main() {
	// Read numbers from the input CSV file.
	numbers, err := readNumbersFromCSV("random_numbers.csv")
	if err != nil {
		log.Fatalf("Error reading numbers: %v", err)
	}

	// Print the original numbers.
	//fmt.Printf("Original numbers: %v\n", numbers)

	// Concurrent quicksort
	start := time.Now()
	runConcurrentQuicksort(numbers)
	elapsed := time.Since(start)

	// Print the sorted numbers.
	//fmt.Printf("Sorted numbers  : %v\n", numbers)

	// Print the time taken for sorting.
	fmt.Printf("Concurrent Quicksort took: %s\n", elapsed)

	// Write the sorted numbers to the output CSV file. Change the filename to yours.
	err = writeNumbersToCSV("out1m.csv", numbers)
	if err != nil {
		log.Fatalf("Error writing numbers: %v", err)
	}
}
