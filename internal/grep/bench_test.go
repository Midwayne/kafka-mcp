package grep

import "testing"

func BenchmarkMatch(b *testing.B) {
	cases := map[string]Spec{
		"fixed_string_raw":      {Patterns: []string{"WMS"}, FixedStrings: true, In: []string{ScopeValue}},
		"regex_raw":             {Patterns: []string{`ERR-\d{3}`}, In: []string{ScopeValue}},
		"ignore_case_raw":       {Patterns: []string{"timeout"}, IgnoreCase: true, In: []string{ScopeValue}},
		"field_scope (decodes)": {Patterns: []string{"^FAILED$"}, In: []string{"value.status"}},
	}

	for name, spec := range cases {
		matcher, err := Compile(spec)
		if err != nil {
			b.Fatal(err)
		}

		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()

			for b.Loop() {
				if ok, _ := matcher.Match(message()); !ok {
					b.Fatal("expected a match")
				}
			}
		})
	}
}
