package acp_test

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

type docsManifest struct {
	SchemaVersion int    `json:"schemaVersion"`
	Repository    string `json:"repository"`
	ProofSources  []struct {
		ID   string `json:"id"`
		Path string `json:"path"`
	} `json:"proofSources"`
	Examples []struct {
		ID             string   `json:"id"`
		Owner          string   `json:"owner"`
		SourcePath     string   `json:"sourcePath"`
		Availability   string   `json:"availability"`
		OfflineCommand string   `json:"offlineCommand"`
		ProofIDs       []string `json:"proofIds"`
	} `json:"examples"`
}

func TestDocumentationExampleArtifacts(t *testing.T) {
	raw, err := os.ReadFile("../testdata/docs/examples.json")
	if err != nil {
		t.Fatal(err)
	}
	var manifest docsManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.SchemaVersion != 1 || manifest.Repository != "acp" {
		t.Fatalf("manifest identity = (%d, %q)", manifest.SchemaVersion, manifest.Repository)
	}
	if len(manifest.Examples) != 4 {
		t.Fatalf("examples = %d, want 4", len(manifest.Examples))
	}
	proofs := make(map[string]bool, len(manifest.ProofSources))
	for _, proof := range manifest.ProofSources {
		if proof.ID == "" || proofs[proof.ID] {
			t.Fatalf("invalid or duplicate proof id %q", proof.ID)
		}
		proofs[proof.ID] = true
		if _, err := os.Stat("../" + proof.Path); err != nil {
			t.Errorf("proof %s: %v", proof.ID, err)
		}
	}
	wf, err := os.ReadFile("../.github/workflows/docs-examples.yml")
	if err != nil {
		t.Fatal(err)
	}
	for _, example := range manifest.Examples {
		if !strings.HasPrefix(example.ID, "example-acp-") {
			t.Errorf("noncanonical id %q", example.ID)
		}
		if len(example.ProofIDs) == 0 {
			t.Errorf("%s has no proofIds", example.ID)
		}
		if example.Owner != "acp" || example.Availability != "source-workspace" {
			t.Errorf("%s has invalid ownership/availability", example.ID)
		}
		if _, err := os.Stat("../" + example.SourcePath); err != nil {
			t.Errorf("%s source: %v", example.ID, err)
		}
		for _, proofID := range example.ProofIDs {
			if !proofs[proofID] {
				t.Errorf("%s references unknown proof %q", example.ID, proofID)
			}
		}
		if !strings.Contains(string(wf), example.OfflineCommand) {
			t.Errorf("workflow missing literal command %q", example.OfflineCommand)
		}
	}
}
