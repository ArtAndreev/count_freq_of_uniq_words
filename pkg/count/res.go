package count

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
)

func WriteResult(w io.Writer, words []WordWithFrequency) error {
	cw := csv.NewWriter(w)

	row := make([]string, 2)
	for _, word := range words {
		row[0] = word.Word
		row[1] = strconv.Itoa(word.Frequency)

		if err := cw.Write(row); err != nil {
			return err
		}
	}

	cw.Flush()
	return cw.Error()
}

func ReadResult(r io.Reader) ([]WordWithFrequency, error) {
	cr := csv.NewReader(r)

	var words []WordWithFrequency
	for i := 0; ; i++ {
		rec, err := cr.Read()
		if err != nil {
			if err == io.EOF {
				return words, nil
			}
			return nil, err
		}

		freq, err := strconv.Atoi(rec[1])
		if err != nil {
			return nil, fmt.Errorf("read line %d: wrong frequency: %w", i, err)
		}

		words = append(words, WordWithFrequency{
			Word:      rec[0],
			Frequency: freq,
		})
	}
}
