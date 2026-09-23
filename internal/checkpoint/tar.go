package checkpoint

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// TarWorkspace creates a gzipped tar archive of workDir and writes it to w.
// Files named ".exit-code" are skipped.
func TarWorkspace(workDir string, w io.Writer) error {
	gw := gzip.NewWriter(w)
	defer gw.Close()

	tw := tar.NewWriter(gw)
	defer tw.Close()

	return filepath.Walk(workDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip .exit-code files.
		if info.Name() == ".exit-code" {
			return nil
		}

		// Skip sockets (not supported by tar).
		if info.Mode()&os.ModeSocket != 0 {
			return nil
		}

		// Build a relative path for the tar header.
		rel, err := filepath.Rel(workDir, path)
		if err != nil {
			return fmt.Errorf("checkpoint: relative path: %w", err)
		}
		// Skip the root directory entry itself.
		if rel == "." {
			return nil
		}

		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return fmt.Errorf("checkpoint: tar header for %s: %w", rel, err)
		}
		header.Name = filepath.ToSlash(rel)

		// Handle symlinks.
		if info.Mode()&os.ModeSymlink != 0 {
			link, err := os.Readlink(path)
			if err != nil {
				return fmt.Errorf("checkpoint: readlink %s: %w", path, err)
			}
			header.Linkname = link
		}

		if err := tw.WriteHeader(header); err != nil {
			return fmt.Errorf("checkpoint: write header %s: %w", rel, err)
		}

		// Only write contents for regular files.
		if !info.Mode().IsRegular() {
			return nil
		}

		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("checkpoint: open %s: %w", path, err)
		}
		defer f.Close()

		if _, err := io.Copy(tw, f); err != nil {
			return fmt.Errorf("checkpoint: copy %s: %w", rel, err)
		}
		return nil
	})
}

// UntarWorkspace extracts a gzipped tar archive from r into destDir.
// It validates each entry path to prevent path traversal attacks.
func UntarWorkspace(destDir string, r io.Reader) error {
	gr, err := gzip.NewReader(r)
	if err != nil {
		return fmt.Errorf("checkpoint: gzip reader: %w", err)
	}
	defer gr.Close()

	tr := tar.NewReader(gr)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("checkpoint: tar next: %w", err)
		}

		// Validate path to prevent traversal.
		if err := validateTarPath(header.Name); err != nil {
			return err
		}

		target := filepath.Join(destDir, filepath.FromSlash(header.Name))

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, os.FileMode(header.Mode)); err != nil {
				return fmt.Errorf("checkpoint: mkdir %s: %w", target, err)
			}
		case tar.TypeReg:
			// Ensure parent directory exists.
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return fmt.Errorf("checkpoint: mkdir parent %s: %w", target, err)
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(header.Mode))
			if err != nil {
				return fmt.Errorf("checkpoint: create %s: %w", target, err)
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return fmt.Errorf("checkpoint: write %s: %w", target, err)
			}
			if err := f.Close(); err != nil {
				return fmt.Errorf("checkpoint: close %s: %w", target, err)
			}
		default:
			// Skip unsupported entry types (symlinks, etc.).
		}
	}
	return nil
}

// validateTarPath rejects paths that could escape the destination directory.
func validateTarPath(name string) error {
	if strings.HasPrefix(name, "/") {
		return fmt.Errorf("checkpoint: illegal absolute path in tar: %s", name)
	}
	for _, component := range strings.Split(filepath.FromSlash(name), string(filepath.Separator)) {
		if component == ".." {
			return fmt.Errorf("checkpoint: path traversal in tar: %s", name)
		}
	}
	return nil
}

// HashFile returns the SHA-256 hash of the file at path in the format "sha256:<hex>".
func HashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("checkpoint: open for hash: %w", err)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("checkpoint: hash copy: %w", err)
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}
