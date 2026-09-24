package runner

import (
	"encoding/json"
	"fmt"
	"os"

	"durarun-operator/internal/protocol"
)

const terminationLogPath = "/dev/termination-log"

// WriteTerminationMessage writes a JSON-encoded ProcessResult to the
// Kubernetes termination-log file so the kubelet can surface it in
// the Pod status.
func WriteTerminationMessage(result *protocol.ProcessResult) error {
	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal termination message: %w", err)
	}
	if err := os.WriteFile(terminationLogPath, data, 0o644); err != nil {
		return fmt.Errorf("write termination log: %w", err)
	}
	return nil
}
