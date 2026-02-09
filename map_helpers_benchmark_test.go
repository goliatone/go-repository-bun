package repository

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

type benchmarkMapModel struct {
	ID        uuid.UUID  `bun:"id,pk,notnull" json:"id"`
	Name      string     `bun:"name,notnull" json:"name"`
	Email     string     `bun:"email,notnull" json:"email"`
	Count     int        `bun:"count,notnull" json:"count"`
	Enabled   *bool      `bun:"enabled" json:"enabled,omitempty"`
	CreatedAt time.Time  `bun:"created_at,notnull" json:"created_at"`
	UpdatedAt *time.Time `bun:"updated_at" json:"updated_at,omitempty"`
}

func BenchmarkRecordToMap_NativeVsJSONRoundtrip(b *testing.B) {
	enabled := true
	now := time.Now().UTC().Truncate(time.Millisecond)
	record := benchmarkMapModel{
		ID:        uuid.New(),
		Name:      "Benchmark User",
		Email:     "benchmark@example.com",
		Count:     42,
		Enabled:   &enabled,
		CreatedAt: now,
		UpdatedAt: &now,
	}

	b.Run("native", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, err := RecordToMap(record)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("json-roundtrip", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := benchmarkJSONRecordToMap(record); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkMapToRecord_NativeVsJSONRoundtrip(b *testing.B) {
	enabled := true
	now := time.Now().UTC().Truncate(time.Millisecond)

	payload := map[string]any{
		"id":         uuid.New().String(),
		"name":       "Benchmark User",
		"email":      "benchmark@example.com",
		"count":      42,
		"enabled":    enabled,
		"created_at": now.Format(time.RFC3339Nano),
		"updated_at": now.Format(time.RFC3339Nano),
	}

	b.Run("native", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, err := MapToRecord[*benchmarkMapModel](payload)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("json-roundtrip", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := benchmarkJSONMapToRecord[benchmarkMapModel](payload); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func benchmarkJSONRecordToMap[T any](record T) (map[string]any, error) {
	data, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}

	out := map[string]any{}
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func benchmarkJSONMapToRecord[T any](payload map[string]any) (T, error) {
	var record T

	data, err := json.Marshal(payload)
	if err != nil {
		return record, err
	}
	if err := json.Unmarshal(data, &record); err != nil {
		return record, err
	}
	return record, nil
}
