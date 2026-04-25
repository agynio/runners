package server

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

type listCursor struct {
	Primary string `json:"primary"`
	ID      string `json:"id"`
}

func encodeListCursor(primary string, id uuid.UUID) (string, error) {
	payload, err := json.Marshal(listCursor{Primary: primary, ID: id.String()})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func decodeListCursor(token string) (listCursor, uuid.UUID, error) {
	if token == "" {
		return listCursor{}, uuid.UUID{}, errors.New("empty token")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return listCursor{}, uuid.UUID{}, fmt.Errorf("decode token: %w", err)
	}
	var cursor listCursor
	if err := json.Unmarshal(decoded, &cursor); err != nil {
		return listCursor{}, uuid.UUID{}, fmt.Errorf("parse token: %w", err)
	}
	if cursor.ID == "" {
		return listCursor{}, uuid.UUID{}, errors.New("invalid token")
	}
	parsedID, err := uuid.Parse(cursor.ID)
	if err != nil {
		return listCursor{}, uuid.UUID{}, fmt.Errorf("parse id: %w", err)
	}
	return cursor, parsedID, nil
}
