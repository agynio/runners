package server

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxCatalogNameLength = 64

var catalogNamePattern = regexp.MustCompile(`^[a-z0-9-]+$`)

// ReportRunnerCatalog replaces a runner's stored catalog with the reported
// state. The report is declarative and validated as a whole: any violation
// rejects it entirely and leaves the previously stored catalog in place, so a
// runner rolling out a bad configuration cannot half-apply it and strand the
// environments that reference the old entries.
func (s *Server) ReportRunnerCatalog(ctx context.Context, req *runnersv1.ReportRunnerCatalogRequest) (*runnersv1.ReportRunnerCatalogResponse, error) {
	serviceToken := strings.TrimSpace(req.GetServiceToken())
	if serviceToken == "" {
		return nil, status.Error(codes.InvalidArgument, "service_token must be provided")
	}
	runner, err := s.getRunnerByServiceTokenHash(ctx, hashServiceToken(serviceToken))
	if err != nil {
		return nil, toStatusError(err)
	}
	// The token identifies the runner; a runner_id that disagrees with it is a
	// misdirected report rather than a permitted cross-runner write.
	if reported := strings.TrimSpace(req.GetRunnerId()); reported != "" {
		reportedID, parseErr := parseUUID(reported)
		if parseErr != nil {
			return nil, status.Errorf(codes.InvalidArgument, "runner_id: %v", parseErr)
		}
		if reportedID != runner.Meta.ID {
			return nil, status.Error(codes.PermissionDenied, "runner_id does not match the reporting runner")
		}
	}

	if err := validateFlavorEntries(req.GetFlavors()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "flavors: %v", err)
	}
	if err := validateStorageClassEntries(req.GetStorageClasses()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "storage_classes: %v", err)
	}
	capabilities, err := validateCapabilities(req.GetCapabilities())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "capabilities: %v", err)
	}

	if err := s.replaceRunnerCatalog(ctx, runner.Meta.ID, req.GetFlavors(), req.GetStorageClasses(), capabilities); err != nil {
		return nil, toStatusError(err)
	}

	return &runnersv1.ReportRunnerCatalogResponse{
		FlavorCount:       int32(len(req.GetFlavors())),
		StorageClassCount: int32(len(req.GetStorageClasses())),
		CapabilityCount:   int32(len(capabilities)),
	}, nil
}

func validateCatalogName(name string) error {
	if name == "" {
		return fmt.Errorf("name is empty")
	}
	if len(name) > maxCatalogNameLength {
		return fmt.Errorf("name %q is longer than %d characters", name, maxCatalogNameLength)
	}
	if !catalogNamePattern.MatchString(name) {
		return fmt.Errorf("name %q must match %s", name, catalogNamePattern.String())
	}
	return nil
}

func validateFlavorEntries(entries []*runnersv1.FlavorEntry) error {
	seen := make(map[string]struct{}, len(entries))
	defaults := 0
	for _, entry := range entries {
		if entry == nil {
			return fmt.Errorf("entry is nil")
		}
		name := strings.TrimSpace(entry.GetName())
		if err := validateCatalogName(name); err != nil {
			return err
		}
		if _, duplicate := seen[name]; duplicate {
			return fmt.Errorf("name %q appears more than once", name)
		}
		seen[name] = struct{}{}
		if entry.GetDefault() {
			defaults++
		}
		if err := validateComputeResources(entry.GetResources()); err != nil {
			return fmt.Errorf("flavor %q: %w", name, err)
		}
	}
	if defaults > 1 {
		return fmt.Errorf("at most one entry may be marked default, got %d", defaults)
	}
	return nil
}

func validateComputeResources(resources *runnersv1.ComputeResources) error {
	if resources == nil {
		return fmt.Errorf("resources must be provided")
	}
	fields := map[string]string{
		"requests_cpu":    resources.GetRequestsCpu(),
		"requests_memory": resources.GetRequestsMemory(),
		"limits_cpu":      resources.GetLimitsCpu(),
		"limits_memory":   resources.GetLimitsMemory(),
	}
	for field, value := range fields {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s must be provided", field)
		}
	}
	return nil
}

func validateStorageClassEntries(entries []*runnersv1.StorageClassEntry) error {
	seen := make(map[string]struct{}, len(entries))
	defaults := 0
	for _, entry := range entries {
		if entry == nil {
			return fmt.Errorf("entry is nil")
		}
		name := strings.TrimSpace(entry.GetName())
		if err := validateCatalogName(name); err != nil {
			return err
		}
		if _, duplicate := seen[name]; duplicate {
			return fmt.Errorf("name %q appears more than once", name)
		}
		seen[name] = struct{}{}
		if entry.GetDefault() {
			defaults++
		}
	}
	if defaults > 1 {
		return fmt.Errorf("at most one entry may be marked default, got %d", defaults)
	}
	return nil
}

func validateCapabilities(capabilities []string) ([]string, error) {
	seen := make(map[string]struct{}, len(capabilities))
	out := make([]string, 0, len(capabilities))
	for _, capability := range capabilities {
		name := strings.TrimSpace(capability)
		if err := validateCatalogName(name); err != nil {
			return nil, err
		}
		if _, duplicate := seen[name]; duplicate {
			return nil, fmt.Errorf("capability %q appears more than once", name)
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	return out, nil
}

// replaceRunnerCatalog swaps the stored catalog for the reported one in a
// single transaction: a report is an atomic statement of what the runner
// offers, and a partially applied one would make names resolve inconsistently
// while it was in progress.
func (s *Server) replaceRunnerCatalog(
	ctx context.Context,
	runnerID uuid.UUID,
	flavors []*runnersv1.FlavorEntry,
	storageClasses []*runnersv1.StorageClassEntry,
	capabilities []string,
) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `DELETE FROM runner_flavors WHERE runner_id = $1`, runnerID); err != nil {
		return fmt.Errorf("clear flavors: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM runner_storage_classes WHERE runner_id = $1`, runnerID); err != nil {
		return fmt.Errorf("clear storage classes: %w", err)
	}

	for _, entry := range flavors {
		resources := entry.GetResources()
		if _, err := tx.Exec(ctx, `
			INSERT INTO runner_flavors (
				runner_id, name, requests_cpu, requests_memory, limits_cpu, limits_memory, is_default, deprecated
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			runnerID,
			strings.TrimSpace(entry.GetName()),
			resources.GetRequestsCpu(),
			resources.GetRequestsMemory(),
			resources.GetLimitsCpu(),
			resources.GetLimitsMemory(),
			entry.GetDefault(),
			entry.GetDeprecated(),
		); err != nil {
			return fmt.Errorf("insert flavor %q: %w", entry.GetName(), err)
		}
	}

	for _, entry := range storageClasses {
		if _, err := tx.Exec(ctx, `
			INSERT INTO runner_storage_classes (runner_id, name, is_default, deprecated)
			VALUES ($1, $2, $3, $4)`,
			runnerID,
			strings.TrimSpace(entry.GetName()),
			entry.GetDefault(),
			entry.GetDeprecated(),
		); err != nil {
			return fmt.Errorf("insert storage class %q: %w", entry.GetName(), err)
		}
	}

	// Capabilities are part of the same declaration, so they are replaced in the
	// same transaction rather than through RegisterRunner/UpdateRunner.
	if _, err := tx.Exec(ctx, `UPDATE runners SET capabilities = $2, updated_at = NOW() WHERE id = $1`,
		runnerID, capabilities); err != nil {
		return fmt.Errorf("update capabilities: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// ListFlavors returns catalog entries, optionally narrowed to one runner.
// Deprecated entries are hidden unless asked for: they still resolve and
// schedule, but pickers should steer new references away from them.
func (s *Server) ListFlavors(ctx context.Context, req *runnersv1.ListFlavorsRequest) (*runnersv1.ListFlavorsResponse, error) {
	runnerID, organizationID, err := s.catalogListScope(ctx, req.RunnerId, req.OrganizationId)
	if err != nil {
		return nil, err
	}

	rows, err := s.pool.Query(ctx, `
		SELECT f.runner_id, f.name, f.requests_cpu, f.requests_memory, f.limits_cpu, f.limits_memory,
		       f.is_default, f.deprecated, r.name, r.organization_id
		FROM runner_flavors f
		JOIN runners r ON r.id = f.runner_id
		WHERE ($1::uuid IS NULL OR f.runner_id = $1)
		  AND ($2::uuid IS NULL OR r.organization_id = $2 OR r.organization_id IS NULL)
		  AND ($3::boolean OR NOT f.deprecated)
		ORDER BY r.name, f.name`,
		runnerID, organizationID, req.GetIncludeDeprecated())
	if err != nil {
		return nil, toStatusError(fmt.Errorf("list flavors: %w", err))
	}
	defer rows.Close()

	flavors := []*runnersv1.Flavor{}
	for rows.Next() {
		var (
			flavor         runnersv1.Flavor
			resources      runnersv1.ComputeResources
			runnerName     string
			organizationID *uuid.UUID
			runnerUUID     uuid.UUID
		)
		if err := rows.Scan(&runnerUUID, &flavor.Name, &resources.RequestsCpu, &resources.RequestsMemory,
			&resources.LimitsCpu, &resources.LimitsMemory, &flavor.Default, &flavor.Deprecated,
			&runnerName, &organizationID); err != nil {
			return nil, toStatusError(fmt.Errorf("scan flavor: %w", err))
		}
		flavor.RunnerId = runnerUUID.String()
		flavor.Resources = &resources
		flavor.RunnerName = runnerName
		if organizationID != nil {
			org := organizationID.String()
			flavor.OrganizationId = &org
		}
		flavors = append(flavors, &flavor)
	}
	if err := rows.Err(); err != nil {
		return nil, toStatusError(fmt.Errorf("list flavors: %w", err))
	}
	return &runnersv1.ListFlavorsResponse{Flavors: flavors}, nil
}

// ListStorageClasses mirrors ListFlavors for the storage half of the catalog.
func (s *Server) ListStorageClasses(ctx context.Context, req *runnersv1.ListStorageClassesRequest) (*runnersv1.ListStorageClassesResponse, error) {
	runnerID, organizationID, err := s.catalogListScope(ctx, req.RunnerId, req.OrganizationId)
	if err != nil {
		return nil, err
	}

	rows, err := s.pool.Query(ctx, `
		SELECT c.runner_id, c.name, c.is_default, c.deprecated, r.name, r.organization_id
		FROM runner_storage_classes c
		JOIN runners r ON r.id = c.runner_id
		WHERE ($1::uuid IS NULL OR c.runner_id = $1)
		  AND ($2::uuid IS NULL OR r.organization_id = $2 OR r.organization_id IS NULL)
		  AND ($3::boolean OR NOT c.deprecated)
		ORDER BY r.name, c.name`,
		runnerID, organizationID, req.GetIncludeDeprecated())
	if err != nil {
		return nil, toStatusError(fmt.Errorf("list storage classes: %w", err))
	}
	defer rows.Close()

	classes := []*runnersv1.StorageClass{}
	for rows.Next() {
		var (
			class          runnersv1.StorageClass
			runnerName     string
			organizationID *uuid.UUID
			runnerUUID     uuid.UUID
		)
		if err := rows.Scan(&runnerUUID, &class.Name, &class.Default, &class.Deprecated,
			&runnerName, &organizationID); err != nil {
			return nil, toStatusError(fmt.Errorf("scan storage class: %w", err))
		}
		class.RunnerId = runnerUUID.String()
		class.RunnerName = runnerName
		if organizationID != nil {
			org := organizationID.String()
			class.OrganizationId = &org
		}
		classes = append(classes, &class)
	}
	if err := rows.Err(); err != nil {
		return nil, toStatusError(fmt.Errorf("list storage classes: %w", err))
	}
	return &runnersv1.ListStorageClassesResponse{StorageClasses: classes}, nil
}

// catalogListScope parses the optional filters and applies the same membership
// check the other read paths use. A caller with no identity is internal (the
// Orchestrator resolving a name at workload start) and is not org-scoped.
func (s *Server) catalogListScope(ctx context.Context, runnerIDArg, organizationIDArg *string) (*uuid.UUID, *uuid.UUID, error) {
	callerID, err := identityFromMetadataOptional(ctx)
	if err != nil {
		return nil, nil, status.Errorf(codes.Unauthenticated, "unauthenticated: %v", err)
	}
	var runnerID *uuid.UUID
	if runnerIDArg != nil && strings.TrimSpace(*runnerIDArg) != "" {
		parsed, parseErr := parseUUID(*runnerIDArg)
		if parseErr != nil {
			return nil, nil, status.Errorf(codes.InvalidArgument, "runner_id: %v", parseErr)
		}
		runnerID = &parsed
	}
	var organizationID *uuid.UUID
	if organizationIDArg != nil && strings.TrimSpace(*organizationIDArg) != "" {
		parsed, parseErr := parseUUID(*organizationIDArg)
		if parseErr != nil {
			return nil, nil, status.Errorf(codes.InvalidArgument, "organization_id: %v", parseErr)
		}
		organizationID = &parsed
	}
	if callerID != nil && organizationID != nil {
		if err := s.requireOrgMember(ctx, *callerID, *organizationID); err != nil {
			return nil, nil, err
		}
	}
	return runnerID, organizationID, nil
}
