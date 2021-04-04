package count

import (
	"bufio"
	"io"
	"sort"
	"strings"
)

// TODO(artandreev): ban not alpha chars.
func WithScanner(r io.Reader) ([]WordWithFrequency, error) {
	s := bufio.NewScanner(r)
	s.Split(bufio.ScanWords) // Unicode (UTF-8) friendly.

	freqByWord := make(map[string]*int)
	for s.Scan() {
		word := strings.ToLower(s.Text())
		incFreq(freqByWord, word)
	}
	if err := s.Err(); err != nil {
		return nil, err
	}

	res := getResult(freqByWord)
	return res, nil
}

func incFreq(m map[string]*int, k string) {
	cnt, ok := m[k]
	if !ok {
		cnt = new(int)
		m[k] = cnt
	}
	*cnt++
}

func addFreq(m map[string]*int, k string, freq int) {
	cnt, ok := m[k]
	if !ok {
		cnt = new(int)
		m[k] = cnt
	}
	*cnt += freq
}

func getResult(freqByWord map[string]*int) []WordWithFrequency {
	res := make([]WordWithFrequency, 0, len(freqByWord))
	for word, freq := range freqByWord {
		res = append(res, WordWithFrequency{
			Word:      word,
			Frequency: *freq, // never nil.
		})
	}

	sort.Slice(res, func(i, j int) bool {
		if res[i].Frequency > res[j].Frequency {
			return true
		}
		if res[i].Frequency < res[j].Frequency {
			return false
		}
		return res[i].Word < res[j].Word
	})

	return res
}
