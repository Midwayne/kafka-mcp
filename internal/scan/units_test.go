package scan

import (
	"testing"
	"time"
)

func TestParsePoint(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	cases := map[string]Point{
		"":                          {Kind: Earliest},
		"earliest":                  {Kind: Earliest},
		"LATEST":                    {Kind: Latest},
		"now":                       {Kind: Latest},
		"-15m":                      {Kind: AtTime, Millis: now.Add(-15 * time.Minute).UnixMilli()},
		"-2d":                       {Kind: AtTime, Millis: now.Add(-48 * time.Hour).UnixMilli()},
		"3h ago":                    {Kind: AtTime, Millis: now.Add(-3 * time.Hour).UnixMilli()},
		"1 h ago":                   {Kind: AtTime, Millis: now.Add(-time.Hour).UnixMilli()},
		"2026-05-31T10:00:00Z":      {Kind: AtTime, Millis: time.Date(2026, 5, 31, 10, 0, 0, 0, time.UTC).UnixMilli()},
		"2026-05-31T10:00:00+02:00": {Kind: AtTime, Millis: time.Date(2026, 5, 31, 8, 0, 0, 0, time.UTC).UnixMilli()},
		"2026-05-31 10:00:00":       {Kind: AtTime, Millis: time.Date(2026, 5, 31, 10, 0, 0, 0, time.UTC).UnixMilli()},
		"2026-05-31":                {Kind: AtTime, Millis: time.Date(2026, 5, 31, 0, 0, 0, 0, time.UTC).UnixMilli()},
		"1780000000":                {Kind: AtTime, Millis: 1780000000000},
		"1780000000123":             {Kind: AtTime, Millis: 1780000000123},
	}

	for raw, want := range cases {
		got, err := ParsePoint(raw, now)
		if err != nil || got != want {
			t.Fatalf("ParsePoint(%q) = %+v, %v; want %+v", raw, got, err, want)
		}
	}

	for _, bad := range []string{"yesterday", "-abc", "2026-13-01"} {
		if _, err := ParsePoint(bad, now); err == nil {
			t.Fatalf("ParsePoint(%q) should fail", bad)
		}
	}
}

func TestMurmur2MatchesKafkaTestVectors(t *testing.T) {
	// From org.apache.kafka.common.utils.UtilsTest#testMurmur2.
	vectors := map[string]int32{
		"21":                         -973932308,
		"foobar":                     -790332482,
		"a-little-bit-long-string":   -985981536,
		"a-little-bit-longer-string": -1486304829,
		"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8": -58897971,
		"abc": 479470107,
	}

	for input, want := range vectors {
		if got := murmur2([]byte(input)); got != want {
			t.Fatalf("murmur2(%q) = %d, want %d", input, got, want)
		}
	}
}

func TestPartitionerByName(t *testing.T) {
	for _, name := range []string{"murmur2", "java", "crc32", "librdkafka", "fnv1a", "Sarama"} {
		partitioner, err := PartitionerByName(name)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}

		for _, key := range []string{"", "a", "order-123", "\xff\xfe"} {
			if p := partitioner([]byte(key), 7); p < 0 || p >= 7 {
				t.Fatalf("%s(%q) = %d out of range", name, key, p)
			}
		}
	}

	if _, err := PartitionerByName("random"); err == nil {
		t.Fatal("expected unknown partitioner error")
	}
}
