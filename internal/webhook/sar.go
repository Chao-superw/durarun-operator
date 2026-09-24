package webhook

import (
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CheckSAR verifies the requesting user has permission to use the specified
// resources. This is a placeholder implementation that validates the service
// account name is not empty. A production implementation would perform an
// actual SubjectAccessReview against the API server.
func CheckSAR(_ context.Context, _ client.Client, _ string, sa string) error {
	if sa == "" {
		return fmt.Errorf("service account name must not be empty")
	}
	return nil
}
