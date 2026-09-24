package executor

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"durarun-operator/api/v1alpha1"
)

// BuildAttemptSecret creates a short-lived Secret containing an attempt-scoped
// token for artifact gateway authentication. The token is mounted into the
// sandbox pod at /var/run/durarun/token.
func BuildAttemptSecret(job *v1alpha1.AgentJob, attempt *v1alpha1.AgentAttempt, token string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-token", attempt.Name),
			Namespace: job.Namespace,
			Labels: map[string]string{
				"durarun.io/job":     job.Name,
				"durarun.io/attempt": attempt.Name,
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"token": []byte(token),
		},
	}
}
