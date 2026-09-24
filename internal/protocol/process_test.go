package protocol

import "testing"

func TestProcessResult_Success(t *testing.T) {
	tests := []struct {
		name   string
		result ProcessResult
		want   bool
	}{
		{
			name:   "zero exit, no signal, no error",
			result: ProcessResult{ExitCode: 0},
			want:   true,
		},
		{
			name:   "non-zero exit code",
			result: ProcessResult{ExitCode: 1},
			want:   false,
		},
		{
			name:   "killed by signal",
			result: ProcessResult{ExitCode: 0, Signal: 9},
			want:   false,
		},
		{
			name:   "error string present",
			result: ProcessResult{ExitCode: 0, Error: "exec failed"},
			want:   false,
		},
		{
			name:   "oom killed but exit 0 no signal no error",
			result: ProcessResult{ExitCode: 0, OOMKilled: true},
			want:   true, // OOMKilled alone does not affect Success()
		},
		{
			name:   "all fields set",
			result: ProcessResult{ExitCode: 137, Signal: 9, OOMKilled: true, Error: "oom"},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.result.Success()
			if got != tt.want {
				t.Errorf("ProcessResult%+v.Success() = %v, want %v", tt.result, got, tt.want)
			}
		})
	}
}
