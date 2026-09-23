//go:build !darwin

package wal

import "os"

func platformSync(f *os.File) error {
	return f.Sync()
}
