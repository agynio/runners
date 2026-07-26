package server

import (
	"strings"
	"testing"

	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
)

func flavorEntry(name string, isDefault bool) *runnersv1.FlavorEntry {
	return &runnersv1.FlavorEntry{
		Name:    name,
		Default: isDefault,
		Resources: &runnersv1.ComputeResources{
			RequestsCpu:    "500m",
			RequestsMemory: "2Gi",
			LimitsCpu:      "1",
			LimitsMemory:   "2Gi",
		},
	}
}

func TestValidateFlavorEntriesAcceptsCatalog(t *testing.T) {
	entries := []*runnersv1.FlavorEntry{
		flavorEntry("ram-2gb", true),
		flavorEntry("ram-4gb", false),
	}

	if err := validateFlavorEntries(entries); err != nil {
		t.Fatalf("validate flavors: %v", err)
	}
}

func TestValidateFlavorEntriesRejectsDuplicateName(t *testing.T) {
	entries := []*runnersv1.FlavorEntry{flavorEntry("ram-2gb", false), flavorEntry("ram-2gb", false)}

	err := validateFlavorEntries(entries)
	if err == nil || !strings.Contains(err.Error(), "more than once") {
		t.Fatalf("expected duplicate name rejection, got %v", err)
	}
}

func TestValidateFlavorEntriesRejectsSecondDefault(t *testing.T) {
	entries := []*runnersv1.FlavorEntry{flavorEntry("ram-2gb", true), flavorEntry("ram-4gb", true)}

	err := validateFlavorEntries(entries)
	if err == nil || !strings.Contains(err.Error(), "at most one entry") {
		t.Fatalf("expected single-default rejection, got %v", err)
	}
}

func TestValidateFlavorEntriesRejectsBadNames(t *testing.T) {
	for _, name := range []string{"", "Ram2GB", "ram_2gb", "ram 2gb", strings.Repeat("a", maxCatalogNameLength+1)} {
		if err := validateFlavorEntries([]*runnersv1.FlavorEntry{flavorEntry(name, false)}); err == nil {
			t.Fatalf("expected name %q to be rejected", name)
		}
	}
}

func TestValidateFlavorEntriesRequiresCompleteResources(t *testing.T) {
	entry := flavorEntry("ram-2gb", false)
	entry.Resources.LimitsMemory = ""

	err := validateFlavorEntries([]*runnersv1.FlavorEntry{entry})
	if err == nil || !strings.Contains(err.Error(), "limits_memory") {
		t.Fatalf("expected missing limits_memory to be rejected, got %v", err)
	}

	missing := flavorEntry("ram-4gb", false)
	missing.Resources = nil
	if err := validateFlavorEntries([]*runnersv1.FlavorEntry{missing}); err == nil {
		t.Fatal("expected absent resources to be rejected")
	}
}

func TestValidateFlavorEntriesAcceptsEmptyCatalog(t *testing.T) {
	// A runner may legitimately report nothing; that is how entries are removed.
	if err := validateFlavorEntries(nil); err != nil {
		t.Fatalf("validate empty flavors: %v", err)
	}
}

func TestValidateStorageClassEntriesEnforcesSameRules(t *testing.T) {
	if err := validateStorageClassEntries([]*runnersv1.StorageClassEntry{
		{Name: "standard", Default: true},
		{Name: "fast-ssd"},
	}); err != nil {
		t.Fatalf("validate storage classes: %v", err)
	}

	if err := validateStorageClassEntries([]*runnersv1.StorageClassEntry{
		{Name: "standard", Default: true},
		{Name: "fast-ssd", Default: true},
	}); err == nil {
		t.Fatal("expected single-default rejection")
	}

	if err := validateStorageClassEntries([]*runnersv1.StorageClassEntry{
		{Name: "standard"},
		{Name: "standard"},
	}); err == nil {
		t.Fatal("expected duplicate name rejection")
	}
}

func TestValidateCapabilitiesNormalizesAndRejectsDuplicates(t *testing.T) {
	got, err := validateCapabilities([]string{" docker ", "gpu"})
	if err != nil {
		t.Fatalf("validate capabilities: %v", err)
	}
	if len(got) != 2 || got[0] != "docker" || got[1] != "gpu" {
		t.Fatalf("expected trimmed capabilities, got %v", got)
	}

	if _, err := validateCapabilities([]string{"docker", "docker"}); err == nil {
		t.Fatal("expected duplicate capability rejection")
	}
	if _, err := validateCapabilities([]string{"Docker"}); err == nil {
		t.Fatal("expected invalid capability name rejection")
	}
}
