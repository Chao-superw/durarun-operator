package wal

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Reader reads and validates WAL files from a directory.
type Reader struct {
	dir string
}

// NewReader opens a WAL directory for reading.
func NewReader(dir string) (*Reader, error) {
	wdir := walDirPath(dir)
	if _, err := os.Stat(wdir); err != nil {
		return nil, fmt.Errorf("wal: open dir: %w", err)
	}
	return &Reader{dir: dir}, nil
}

// ReadAll returns every record across all WAL files, validating sequence
// continuity. Returns ErrCorrupt on gaps or malformed data.
func (rd *Reader) ReadAll() ([]Record, error) {
	wdir := walDirPath(rd.dir)
	files, err := listWALFiles(wdir)
	if err != nil {
		return nil, err
	}

	var records []Record
	for _, fname := range files {
		recs, err := readWALFile(filepath.Join(wdir, fname))
		if err != nil {
			return nil, err
		}
		records = append(records, recs...)
	}

	for i := 0; i < len(records); i++ {
		expected := int64(i + 1)
		if records[i].Seq != expected {
			return nil, fmt.Errorf("%w: expected seq %d, got %d", ErrCorrupt, expected, records[i].Seq)
		}
	}
	return records, nil
}

// BuildManifest scans all records and builds a Manifest summarising
// completed steps (exit code 0).
func (rd *Reader) BuildManifest() (*Manifest, error) {
	records, err := rd.ReadAll()
	if err != nil {
		return nil, err
	}

	wdir := walDirPath(rd.dir)
	files, err := listWALFiles(wdir)
	if err != nil {
		return nil, err
	}

	type stepInfo struct {
		beginSeq int64
		endSeq   int64
		exit     *int
		output   string
	}
	steps := make(map[string]*stepInfo)
	var order []string

	for _, r := range records {
		switch r.Type {
		case TypeStepBegin:
			if _, ok := steps[r.StepID]; !ok {
				order = append(order, r.StepID)
			}
			steps[r.StepID] = &stepInfo{beginSeq: r.Seq}
		case TypeStepEnd:
			if s, ok := steps[r.StepID]; ok {
				s.endSeq = r.Seq
				s.exit = r.Exit
				s.output = r.OutputRef
			}
		}
	}

	m := &Manifest{
		SchemaVersion:   1,
		WALFileCount:    len(files),
		WALTotalRecords: int64(len(records)),
	}

	for _, id := range order {
		s := steps[id]
		if s.endSeq > 0 && s.exit != nil && *s.exit == 0 {
			m.CompletedSteps = append(m.CompletedSteps, CompletedStep{
				StepID:    id,
				Seq:       s.endSeq,
				OutputRef: s.output,
			})
			m.LastCompleted = id
		}
	}

	return m, nil
}

// CompletedStepIDs returns the IDs of all completed steps in order.
func (rd *Reader) CompletedStepIDs() ([]string, error) {
	m, err := rd.BuildManifest()
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(m.CompletedSteps))
	for i, cs := range m.CompletedSteps {
		ids[i] = cs.StepID
	}
	return ids, nil
}

// LastCompletedStep returns the ID of the most recently completed step.
func (rd *Reader) LastCompletedStep() (string, error) {
	m, err := rd.BuildManifest()
	if err != nil {
		return "", err
	}
	return m.LastCompleted, nil
}

// IsStepCompleted reports whether a given step finished successfully.
func (rd *Reader) IsStepCompleted(stepID string) (bool, error) {
	ids, err := rd.CompletedStepIDs()
	if err != nil {
		return false, err
	}
	for _, id := range ids {
		if id == stepID {
			return true, nil
		}
	}
	return false, nil
}

// SaveManifest writes a Manifest to the WAL directory as manifest.json.
func SaveManifest(dir string, m *Manifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(walDirPath(dir), manifestFile), data, 0o644)
}

// LoadManifest reads a previously saved manifest.json from the WAL directory.
func LoadManifest(dir string) (*Manifest, error) {
	data, err := os.ReadFile(filepath.Join(walDirPath(dir), manifestFile))
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// --- helpers ---

func walDirPath(dir string) string {
	return filepath.Join(dir, walDir)
}

func walFileName(num int) string {
	return fmt.Sprintf("wal-%04d.log", num)
}

func listWALFiles(wdir string) ([]string, error) {
	entries, err := os.ReadDir(wdir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var files []string
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, "wal-") && strings.HasSuffix(name, ".log") {
			files = append(files, name)
		}
	}
	sort.Strings(files)
	return files, nil
}

func parseFileNum(name string) int {
	var n int
	fmt.Sscanf(name, "wal-%04d.log", &n)
	return n
}

func readWALFile(path string) ([]Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var records []Record
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var r Record
		if err := json.Unmarshal(line, &r); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrCorrupt, err)
		}
		records = append(records, r)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func lastSeqInDir(wdir string, files []string) (int64, error) {
	var maxSeq int64
	for _, fname := range files {
		recs, err := readWALFile(filepath.Join(wdir, fname))
		if err != nil {
			return 0, err
		}
		for _, r := range recs {
			if r.Seq > maxSeq {
				maxSeq = r.Seq
			}
		}
	}
	return maxSeq, nil
}
