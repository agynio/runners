package server

import (
	"database/sql/driver"
	"fmt"
	"strings"

	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

type nullableUUIDScanner struct {
	UUID  uuid.UUID
	Valid bool
}

func (s *nullableUUIDScanner) Scan(value any) error {
	if value == nil {
		s.UUID = uuid.Nil
		s.Valid = false
		return nil
	}
	s.Valid = true
	switch typed := value.(type) {
	case uuid.UUID:
		s.UUID = typed
		return nil
	case pgtype.UUID:
		if !typed.Valid {
			s.UUID = uuid.Nil
			s.Valid = false
			return nil
		}
		s.UUID = uuid.UUID(typed.Bytes)
		return nil
	case string:
		parsed, err := uuid.Parse(typed)
		if err != nil {
			return err
		}
		s.UUID = parsed
		return nil
	case []byte:
		parsed, err := uuid.ParseBytes(typed)
		if err != nil {
			return err
		}
		s.UUID = parsed
		return nil
	default:
		return fmt.Errorf("cannot scan %T as uuid", value)
	}
}

func (s nullableUUIDScanner) Value() (driver.Value, error) {
	if !s.Valid {
		return nil, nil
	}
	return s.UUID.String(), nil
}

const (
	runtimeOwnerKindAgentInstance = "agent_instance"
	runtimeOwnerKindSandbox       = "sandbox"
)

func runtimeOwnerKindToString(kind runnersv1.RuntimeOwnerKind) (string, error) {
	switch kind {
	case runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_UNSPECIFIED,
		runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE:
		return runtimeOwnerKindAgentInstance, nil
	case runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX:
		return runtimeOwnerKindSandbox, nil
	default:
		return "", fmt.Errorf("%s", kind.String())
	}
}

func runtimeOwnerKindFromString(kind string) (runnersv1.RuntimeOwnerKind, error) {
	switch kind {
	case runtimeOwnerKindAgentInstance:
		return runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE, nil
	case runtimeOwnerKindSandbox:
		return runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX, nil
	default:
		return runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_UNSPECIFIED, fmt.Errorf("unknown runtime owner kind %q", kind)
	}
}

func parseOptionalUUID(value string) (*uuid.UUID, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil, nil
	}
	id, err := uuid.Parse(trimmed)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

func nullableUUIDValue(id *uuid.UUID) any {
	if id == nil {
		return nil
	}
	return *id
}

func pgUUIDValue(id uuid.UUID) pgtype.UUID {
	if id == uuid.Nil {
		return pgtype.UUID{}
	}
	return pgtype.UUID{Bytes: id, Valid: true}
}

func uuidFromPgtype(value pgtype.UUID) *uuid.UUID {
	if !value.Valid {
		return nil
	}
	id := uuid.UUID(value.Bytes)
	return &id
}

func scanNullableUUID(value any) (*uuid.UUID, error) {
	var scanner nullableUUIDScanner
	if err := scanner.Scan(value); err != nil {
		return nil, err
	}
	if !scanner.Valid {
		return nil, nil
	}
	return &scanner.UUID, nil
}

func uuidString(id *uuid.UUID) string {
	if id == nil {
		return ""
	}
	return id.String()
}
