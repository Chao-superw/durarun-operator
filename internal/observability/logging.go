package observability

import (
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// LogJobEvent emits a structured log entry for an AgentJob lifecycle event.
func LogJobEvent(logger logr.Logger, job client.Object, event string, keysAndValues ...interface{}) {
	kv := []interface{}{
		"job", job.GetName(),
		"namespace", job.GetNamespace(),
		"uid", job.GetUID(),
		"event", event,
	}
	kv = append(kv, keysAndValues...)
	logger.Info("job event", kv...)
}

// LogAttemptEvent emits a structured log entry for an AgentAttempt lifecycle event.
func LogAttemptEvent(logger logr.Logger, attempt client.Object, event string, keysAndValues ...interface{}) {
	kv := []interface{}{
		"attempt", attempt.GetName(),
		"namespace", attempt.GetNamespace(),
		"uid", attempt.GetUID(),
		"event", event,
	}
	kv = append(kv, keysAndValues...)
	logger.Info("attempt event", kv...)
}

// RecordEvent creates a Kubernetes event on the given object.
func RecordEvent(recorder record.EventRecorder, obj runtime.Object, eventType, reason, message string) {
	recorder.Event(obj, eventType, reason, message)
}
