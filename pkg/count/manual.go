package count

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"log"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"golang.org/x/sync/errgroup"
)

// From the worst to the best. All solutions are Unicode (UTF-8) friendly.

// XXX(artandreev): looks like good size for chunk.
const chunkSize = 64 << 10 // 64 KB.

func WithManualParsingBufioReader(r io.Reader) ([]WordWithFrequency, error) {
	var word []rune
	freqByWord := make(map[string]*int)
	br := bufio.NewReader(r)
	for {
		r, _, err := br.ReadRune()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err // never happens.
		}

		if unicode.IsSpace(r) {
			if len(word) != 0 {
				incFreq(freqByWord, strings.ToLower(string(word)))
				word = word[:0]
			}
		} else {
			word = append(word, r)
		}
	}
	if len(word) != 0 {
		incFreq(freqByWord, strings.ToLower(string(word)))
	}

	res := getResult(freqByWord)
	return res, nil
}

func WithManualParsingBytesReader(r io.Reader) ([]WordWithFrequency, error) {
	buf := make([]byte, chunkSize)
	var word []rune
	freqByWord := make(map[string]*int)
	br := bytes.NewReader(nil)
	for {
		n, err := r.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		br.Reset(buf[:n])
		for {
			r, _, err := br.ReadRune()
			if err != nil {
				if err == io.EOF {
					break
				}
				return nil, err // never happens.
			}

			if unicode.IsSpace(r) {
				if len(word) != 0 {
					incFreq(freqByWord, strings.ToLower(string(word)))
					word = word[:0]
				}
			} else {
				word = append(word, r)
			}
		}
	}
	if len(word) != 0 {
		incFreq(freqByWord, strings.ToLower(string(word)))
	}

	res := getResult(freqByWord)
	return res, nil
}

func WithManualParsing(r io.Reader) ([]WordWithFrequency, error) {
	buf := make([]byte, chunkSize)
	var word []byte
	freqByWord := make(map[string]*int)
	for {
		n, err := r.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		for i := 0; i < n; {
			r, size := utf8.DecodeRune(buf[i:])

			if unicode.IsSpace(r) {
				if len(word) != 0 {
					incFreq(freqByWord, strings.ToLower(string(word)))
					word = word[:0]
				}
			} else {
				word = append(word, buf[i:i+size]...)
			}

			i += size
		}
	}
	if len(word) != 0 {
		incFreq(freqByWord, strings.ToLower(string(word)))
	}

	res := getResult(freqByWord)
	return res, nil
}

func WithManualParsingNoWordAlloc(r io.Reader) ([]WordWithFrequency, error) {
	buf := make([]byte, chunkSize)
	var reminderFromPrevRead []byte
	var wordStart int
	freqByWord := make(map[string]*int)
	for {
		n, err := r.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		for i := 0; i < n; {
			r, size := utf8.DecodeRune(buf[i:])

			if unicode.IsSpace(r) {
				if len(reminderFromPrevRead) != 0 {
					incFreq(freqByWord, strings.ToLower(string(reminderFromPrevRead)))
					reminderFromPrevRead = reminderFromPrevRead[:0]
				} else {
					if wordStart != i+size-1 {
						incFreq(freqByWord, strings.ToLower(string(buf[wordStart:i])))
					}
				}
				wordStart = i + 1
			} else {
				if len(reminderFromPrevRead) != 0 {
					reminderFromPrevRead = append(reminderFromPrevRead, buf[i:i+size]...)
				}
			}

			i += size
		}

		reminderFromPrevRead = append(reminderFromPrevRead, buf[wordStart:n]...)
		wordStart = 0
	}
	if len(reminderFromPrevRead) != 0 {
		incFreq(freqByWord, strings.ToLower(string(reminderFromPrevRead)))
	}

	res := getResult(freqByWord)
	return res, nil
}

type splitPart struct {
	num         int
	begin       []byte
	reminder    []byte
	chunkIsWord bool
}

// TODO(artandreev): optimize, there is much wait time in some workers interaction.
func WithManualParsingNoWordAllocParallel(r io.Reader) ([]WordWithFrequency, error) {
	var (
		in  = make(chan chunk)
		out = make(chan workerWords)

		chunkPool = sync.Pool{New: func() interface{} {
			c := make([]byte, chunkSize)
			return &c
		}}

		g, ctx = errgroup.WithContext(context.Background())
	)

	g.Go(func() error {
		defer close(in)

		for i := 0; ; i++ {
			start := time.Now()

			buf := chunkPool.Get().(*[]byte)

			n, err := r.Read(*buf)
			if err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}

			waitStart := time.Now()
			select {
			case in <- chunk{
				num:  i,
				data: buf,
				n:    n,
			}:
			case <-ctx.Done():
				return ctx.Err()
			}

			log.Printf("reading: %s, waited for input: %s", time.Since(start), time.Since(waitStart))
		}
	})

	var wg sync.WaitGroup
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		g.Go(func() error {
			defer wg.Done()
			return runWorker(ctx, in, out)
		})
	}
	go func() {
		wg.Wait()
		close(out)
	}()

	freqByWord := make(map[string]*int)
	var splitParts []splitPart
	g.Go(func() error {
		for res := range out {
			start := time.Now()

			for k, v := range res.freqByWord {
				addFreq(freqByWord, k, *v)
			}
			splitParts = append(splitParts, splitPart{
				num:         res.num,
				begin:       append([]byte(nil), res.begin...),
				reminder:    append([]byte(nil), res.reminder...),
				chunkIsWord: res.chunkIsWord,
			})
			chunkPool.Put(res.data)

			log.Printf("writing chunk parsing result: %s", time.Since(start))
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	mergeSplitParts(splitParts, freqByWord)

	res := getResult(freqByWord)
	return res, nil
}

func mergeSplitParts(splitParts []splitPart, freqByWord map[string]*int) {
	sort.Slice(splitParts, func(i, j int) bool {
		return splitParts[i].num < splitParts[j].num
	})
	var word []byte
	for _, part := range splitParts {
		word = append(word, part.begin...)
		if !part.chunkIsWord { // Flush accumulated string.
			if len(word) != 0 {
				incFreq(freqByWord, string(bytes.ToLower(word)))
				word = word[:0]
			}
		}
		word = append(word, part.reminder...)
	}
	if len(word) != 0 {
		incFreq(freqByWord, string(bytes.ToLower(word)))
	}
}

type chunk struct {
	num  int
	data *[]byte
	n    int
}

type workerWords struct {
	num         int
	data        *[]byte
	freqByWord  map[string]*int
	begin       []byte
	reminder    []byte
	chunkIsWord bool
}

func runWorker(ctx context.Context, in <-chan chunk, out chan<- workerWords) error {
	for ch := range in {
		start := time.Now()

		buf := *ch.data

		var (
			begin    []byte
			reminder []byte
		)
		var wordStart int
		var beginMet bool
		chunkIsWord := true
		freqByWord := make(map[string]*int)

		for i := 0; i < ch.n; {
			r, size := utf8.DecodeRune(buf[i:])

			if unicode.IsSpace(r) {
				if wordStart != i+size-1 {
					if beginMet {
						incFreq(freqByWord, strings.ToLower(string(buf[wordStart:i])))
					} else {
						begin = buf[wordStart:i]
					}
				}
				beginMet = true
				chunkIsWord = false
				wordStart = i + 1
			}

			i += size
		}

		if beginMet {
			reminder = buf[wordStart:ch.n]
		} else {
			begin = buf[wordStart:ch.n]
		}

		waitStart := time.Now()
		select {
		case out <- workerWords{
			num:         ch.num,
			data:        ch.data,
			freqByWord:  freqByWord,
			begin:       begin,
			reminder:    reminder,
			chunkIsWord: chunkIsWord,
		}:
		case <-ctx.Done():
			return ctx.Err()
		}

		log.Printf("parsing chunk: %s, waited for output: %s", time.Since(start), time.Since(waitStart))
	}

	return nil
}
