package main

import (
	"log"
	"os"

	"github.com/ArtAndreev/count_freq_of_uniq_words/pkg/count"
)

func main() {
	rf, err := os.Open("pkg/count/testdata/bible.txt")
	if err != nil {
		log.Fatalf("failed to open read file: %s", err)
	}
	defer rf.Close()

	freq, err := count.WithScanner(rf)
	if err != nil {
		log.Fatalf("failed to count freq: %s", err)
	}

	wf, err := os.Create("pkg/count/testdata/bible_freq.csv")
	if err != nil {
		log.Fatalf("failed to open write file: %s", err)
	}
	defer wf.Close()

	if err = count.WriteResult(wf, freq); err != nil {
		log.Fatalf("failed to write result: %s", err)
	}
}
