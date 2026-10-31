package filter

import (
	"testing"
)

// Each iteration builds a fresh message, so decoding cost is included as it is in a scan.
func BenchmarkMatch(b *testing.B) {
	cases := map[string]string{
		"headers_only (no value decode)": `headers["type"] == "OrderPlaced"`,
		"value_field":                    `value.status == "FAILED" && value.amount > 1000`,
		"optional_field":                 `value.?missing.orValue(0) > 5`,
	}

	for name, source := range cases {
		expression, err := CompilePredicate(source)
		if err != nil {
			b.Fatal(err)
		}

		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()

			for b.Loop() {
				if _, err := expression.Match(message(order)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
