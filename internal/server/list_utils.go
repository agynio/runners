package server

import "github.com/google/uuid"

func uniqueUUIDs(values []uuid.UUID) []uuid.UUID {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[uuid.UUID]struct{}, len(values))
	unique := make([]uuid.UUID, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		unique = append(unique, value)
	}
	return unique
}

func compareUUID(a, b uuid.UUID) int {
	aValue := a.String()
	bValue := b.String()
	if aValue < bValue {
		return -1
	}
	if aValue > bValue {
		return 1
	}
	return 0
}
