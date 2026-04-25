package server

import (
	"fmt"

	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
)

type sortDirection string

const (
	sortAsc  sortDirection = "ASC"
	sortDesc sortDirection = "DESC"
)

func parseSortDirection(direction runnersv1.SortDirection, defaultDirection sortDirection) (sortDirection, error) {
	switch direction {
	case runnersv1.SortDirection_SORT_DIRECTION_UNSPECIFIED:
		return defaultDirection, nil
	case runnersv1.SortDirection_SORT_DIRECTION_ASC:
		return sortAsc, nil
	case runnersv1.SortDirection_SORT_DIRECTION_DESC:
		return sortDesc, nil
	default:
		return "", fmt.Errorf("invalid sort direction: %s", direction.String())
	}
}

func (d sortDirection) invert() sortDirection {
	if d == sortAsc {
		return sortDesc
	}
	return sortAsc
}
