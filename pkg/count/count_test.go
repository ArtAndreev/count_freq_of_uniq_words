package count

import (
	"io"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

const testDataFilePath = "testdata/bible.txt" // 4047392 bytes (3.95 MB).

var _bibleFreq []WordWithFrequency

func TestMain(m *testing.M) {
	loadBibleFreq()

	log.SetOutput(io.Discard)

	os.Exit(m.Run())
}

func loadBibleFreq() {
	f, err := os.Open("testdata/bible_freq.csv")
	if err != nil {
		log.Fatalf("failed to open: %s", err)
	}
	defer f.Close()

	if _bibleFreq, err = ReadResult(f); err != nil {
		log.Fatalf("failed to read result: %s", err)
	}
}

func TestWith(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(r io.Reader) ([]WordWithFrequency, error)
	}{
		{
			name: "WithScanner",
			fn:   WithScanner,
		},
		{
			name: "WithManualParsingBufioReader",
			fn:   WithManualParsingBufioReader,
		},
		{
			name: "WithManualParsingBytesReader",
			fn:   WithManualParsingBytesReader,
		},
		{
			name: "WithManualParsing",
			fn:   WithManualParsing,
		},
		{
			name: "WithManualParsingNoWordAlloc",
			fn:   WithManualParsingNoWordAlloc,
		},
		{
			name: "WithManualParsingNoWordAllocParallel",
			fn:   WithManualParsingNoWordAllocParallel,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			f, err := os.Open(testDataFilePath)
			if err != nil {
				t.Fatalf("cannot open %s: %s", testDataFilePath, err)
			}
			defer f.Close()

			words, err := test.fn(f)
			assert.NoError(t, err)
			assert.Equal(t, _bibleFreq, words, "fuck", len(_bibleFreq), len(words))
		})
	}
}

func BenchmarkWith(b *testing.B) {
	benchmarks := []struct {
		name string
		fn   func(r io.Reader) ([]WordWithFrequency, error)
	}{
		{
			name: "WithScanner",
			fn:   WithScanner,
		},
		{
			name: "WithManualParsingBufioReader",
			fn:   WithManualParsingBufioReader,
		},
		{
			name: "WithManualParsingBytesReader",
			fn:   WithManualParsingBytesReader,
		},
		{
			name: "WithManualParsing",
			fn:   WithManualParsing,
		},
		{
			name: "WithManualParsingNoWordAlloc",
			fn:   WithManualParsingNoWordAlloc,
		},
		{
			name: "WithManualParsingNoWordAllocParallel",
			fn:   WithManualParsingNoWordAllocParallel,
		},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			f, err := os.Open(testDataFilePath)
			if err != nil {
				b.Fatalf("cannot open %s: %s", testDataFilePath, err)
			}
			defer f.Close()

			for i := 0; i < b.N; i++ {
				_, err = bm.fn(f)
				if err != nil {
					b.Fatalf("scanner error: %s", err)
				}

				if _, err = f.Seek(0, io.SeekStart); err != nil {
					b.Fatalf("failed to seek to start: %s", err)
				}
			}
		})
	}
}

func BenchmarkMapIncPointer(b *testing.B) {
	m := make(map[int]*int)
	for i := 0; i < b.N; i++ {
		cnt, ok := m[0]
		if !ok {
			cnt = new(int)
			m[0] = cnt
		}
		*cnt++
	}
}

func BenchmarkMapIncPointerInit(b *testing.B) {
	m := make(map[int]*int)
	for i := 0; i < b.N; i++ {
		cnt, ok := m[0]
		if !ok {
			one := 1
			m[0] = &one
			continue
		}
		*cnt++
	}
}

func BenchmarkMapIncNotPointer(b *testing.B) {
	m := make(map[int]int)
	for i := 0; i < b.N; i++ {
		m[0]++
	}
}
