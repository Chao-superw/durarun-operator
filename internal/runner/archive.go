package runner

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// maxArchiveDefaultBytes is the default maximum size for an archive (1 GiB).
const maxArchiveDefaultBytes int64 = 1 << 30

// CreateArchive creates a tar.gz archive of the given directory.
// Security: validates no path traversal, no symlinks to outside workDir, limits total size.
func CreateArchive(workDir string, writer io.Writer, maxBytes int64) error {
	if maxBytes <= 0 {
		maxBytes = maxArchiveDefaultBytes
	}

	absWorkDir, err := filepath.Abs(workDir)
	if err != nil {
		return fmt.Errorf("archive: abs path: %w", err)
	}

	lw := &limitWriter{w: writer, remaining: maxBytes}

	gw := gzip.NewWriter(lw)

	tw := tar.NewWriter(gw)

	walkErr := filepath.Walk(absWorkDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		rel, err := filepath.Rel(absWorkDir, path)
		if err != nil {
			return fmt.Errorf("archive: relative path: %w", err)
		}
		// Skip root directory entry.
		if rel == "." {
			return nil
		}

		// Security: reject symlinks that point outside the workDir.
		if info.Mode()&os.ModeSymlink != 0 {
			linkTarget, err := os.Readlink(path)
			if err != nil {
				return fmt.Errorf("archive: readlink %s: %w", path, err)
			}
			absTarget := linkTarget
			if !filepath.IsAbs(absTarget) {
				absTarget = filepath.Join(filepath.Dir(path), linkTarget)
			}
			absTarget, err = filepath.Abs(absTarget)
			if err != nil {
				return fmt.Errorf("archive: abs symlink target: %w", err)
			}
			if !strings.HasPrefix(absTarget, absWorkDir+string(filepath.Separator)) && absTarget != absWorkDir {
				return fmt.Errorf("archive: symlink %s points outside workdir to %s", rel, absTarget)
			}
		}

		// Skip sockets (not supported by tar).
		if info.Mode()&os.ModeSocket != 0 {
			return nil
		}

		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return fmt.Errorf("archive: tar header for %s: %w", rel, err)
		}
		header.Name = filepath.ToSlash(rel)

		// For symlinks, store the link target.
		if info.Mode()&os.ModeSymlink != 0 {
			link, err := os.Readlink(path)
			if err != nil {
				return fmt.Errorf("archive: readlink %s: %w", path, err)
			}
			header.Linkname = link
		}

		if err := tw.WriteHeader(header); err != nil {
			return fmt.Errorf("archive: write header %s: %w", rel, err)
		}

		// Only write contents for regular files.
		if !info.Mode().IsRegular() {
			return nil
		}

		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("archive: open %s: %w", path, err)
		}
		defer f.Close()

		if _, err := io.Copy(tw, f); err != nil {
			return fmt.Errorf("archive: copy %s: %w", rel, err)
		}
		return nil
	})

	if walkErr != nil {
		// Best-effort close to flush partial data.
		tw.Close()
		gw.Close()
		return walkErr
	}

	if err := tw.Close(); err != nil {
		gw.Close()
		return fmt.Errorf("archive: close tar: %w", err)
	}
	if err := gw.Close(); err != nil {
		return fmt.Errorf("archive: close gzip: %w", err)
	}

	return nil
}

// ExtractArchive extracts a tar.gz archive to the target directory.
// Security: validates all paths are within target, rejects absolute paths and ../ traversal.
func ExtractArchive(reader io.Reader, targetDir string, maxBytes int64) error {
	if maxBytes <= 0 {
		maxBytes = maxArchiveDefaultBytes
	}

	absTarget, err := filepath.Abs(targetDir)
	if err != nil {
		return fmt.Errorf("extract: abs target: %w", err)
	}

	lr := &io.LimitedReader{R: reader, N: maxBytes}

	gr, err := gzip.NewReader(lr)
	if err != nil {
		return fmt.Errorf("extract: gzip reader: %w", err)
	}
	defer gr.Close()

	tr := tar.NewReader(gr)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("extract: tar next: %w", err)
		}

		// Security: reject absolute paths.
		if filepath.IsAbs(header.Name) {
			return fmt.Errorf("extract: illegal absolute path in tar: %s", header.Name)
		}

		// Security: reject path traversal.
		cleanName := filepath.FromSlash(header.Name)
		for _, component := range strings.Split(cleanName, string(filepath.Separator)) {
			if component == ".." {
				return fmt.Errorf("extract: path traversal in tar: %s", header.Name)
			}
		}

		target := filepath.Join(absTarget, cleanName)

		// Double-check resolved path stays within target.
		if !strings.HasPrefix(target, absTarget+string(filepath.Separator)) && target != absTarget {
			return fmt.Errorf("extract: path escape detected: %s resolves to %s", header.Name, target)
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, os.FileMode(header.Mode)); err != nil {
				return fmt.Errorf("extract: mkdir %s: %w", target, err)
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return fmt.Errorf("extract: mkdir parent %s: %w", target, err)
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(header.Mode))
			if err != nil {
				return fmt.Errorf("extract: create %s: %w", target, err)
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return fmt.Errorf("extract: write %s: %w", target, err)
			}
			if err := f.Close(); err != nil {
				return fmt.Errorf("extract: close %s: %w", target, err)
			}
		default:
			// Skip unsupported entry types (symlinks, etc.).
		}
	}
	return nil
}

// limitWriter wraps an io.Writer and limits total bytes written.
type limitWriter struct {
	w         io.Writer
	remaining int64
}

func (lw *limitWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > lw.remaining {
		return 0, fmt.Errorf("archive: exceeded maximum size limit")
	}
	n, err := lw.w.Write(p)
	lw.remaining -= int64(n)
	return n, err
}
