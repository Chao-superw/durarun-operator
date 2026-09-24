package artifact

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/types"
)

// ArtifactKey generates a namespaced, UID-isolated key for an artifact.
// Format: {namespace}/{jobUID}/{attemptUID}/{filename}
func ArtifactKey(namespace string, jobUID, attemptUID types.UID, filename string) string {
	return fmt.Sprintf("%s/%s/%s/%s", namespace, jobUID, attemptUID, filename)
}

// CheckpointKey generates a key for checkpoints.
// Format: {namespace}/{jobUID}/checkpoints/{ordinal}/{filename}
func CheckpointKey(namespace string, jobUID types.UID, ordinal int32, filename string) string {
	return fmt.Sprintf("%s/%s/checkpoints/%d/%s", namespace, jobUID, ordinal, filename)
}

// ManifestKey generates the key for the attempt manifest.
// Format: {namespace}/{jobUID}/{attemptUID}/manifest.json
func ManifestKey(namespace string, jobUID, attemptUID types.UID) string {
	return fmt.Sprintf("%s/%s/%s/manifest.json", namespace, jobUID, attemptUID)
}

// ParseKey extracts namespace and jobUID from a key.
// The remaining path segments are returned as rest.
func ParseKey(key string) (namespace string, jobUID string, rest string, err error) {
	parts := strings.SplitN(key, "/", 3)
	if len(parts) < 3 {
		return "", "", "", fmt.Errorf("invalid key format: expected at least 3 path segments, got %d in %q", len(parts), key)
	}
	return parts[0], parts[1], parts[2], nil
}
