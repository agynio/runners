package server

import (
	"testing"

	"github.com/google/uuid"
)

func TestNullableUUIDScannerScansRawByteArray(t *testing.T) {
	id := uuid.New()
	var raw [16]byte
	copy(raw[:], id[:])

	var scanner nullableUUIDScanner
	if err := scanner.Scan(raw); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if !scanner.Valid {
		t.Fatalf("expected scanner to be valid")
	}
	if scanner.UUID != id {
		t.Fatalf("expected %s, got %s", id, scanner.UUID)
	}
}

func TestScanNullableUUIDScansRawBytes(t *testing.T) {
	id := uuid.New()

	got, err := scanNullableUUID(id[:])
	if err != nil {
		t.Fatalf("scanNullableUUID failed: %v", err)
	}
	if got == nil {
		t.Fatalf("expected uuid")
	}
	if *got != id {
		t.Fatalf("expected %s, got %s", id, *got)
	}
}
