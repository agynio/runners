package server

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type scanFunc[T any] func(pgx.Row) (T, error)
type recordIDFunc[T any] func(T) uuid.UUID

func listWithPagination[T any](
	ctx context.Context,
	pool dbPool,
	baseQuery string,
	clauses []string,
	args []any,
	pageSize int32,
	pageToken string,
	scan scanFunc[T],
	recordID recordIDFunc[T],
) ([]T, string, error) {
	limit := normalizePageSize(pageSize)

	if pageToken != "" {
		afterID, err := decodePageToken(pageToken)
		if err != nil {
			return nil, "", InvalidPageToken(err)
		}
		clauses = append(clauses, fmt.Sprintf("id > $%d", len(args)+1))
		args = append(args, afterID)
	}

	query := strings.Builder{}
	query.WriteString(baseQuery)
	if len(clauses) > 0 {
		query.WriteString(" WHERE ")
		query.WriteString(strings.Join(clauses, " AND "))
	}
	query.WriteString(fmt.Sprintf(" ORDER BY id ASC LIMIT $%d", len(args)+1))
	args = append(args, int(limit)+1)

	rows, err := pool.Query(ctx, query.String(), args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	records := make([]T, 0, limit)
	var (
		lastID  uuid.UUID
		hasMore bool
	)
	for rows.Next() {
		if int32(len(records)) == limit {
			hasMore = true
			break
		}
		record, err := scan(rows)
		if err != nil {
			return nil, "", err
		}
		records = append(records, record)
		lastID = recordID(record)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	nextToken := ""
	if hasMore {
		nextToken = encodePageToken(lastID)
	}
	return records, nextToken, nil
}
