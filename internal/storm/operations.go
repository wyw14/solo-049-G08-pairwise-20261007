package storm

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// OperationLog is an append-only, fsync-per-record JSONL journal of storm
// dispositions. Records are never rewritten or deleted; on startup the log is
// replayed so already-handled weather samples cannot trigger a second stow.
type OperationLog struct {
	path string
	mu   sync.Mutex
	seen map[string]string // sample fingerprint -> operation ID
}

func NewOperationLog(path string) (*OperationLog, error) {
	log := &OperationLog{path: path, seen: make(map[string]string)}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create storm operation directory: %w", err)
	}
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return log, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open storm operation log: %w", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		var operation Operation
		if err := json.Unmarshal(scanner.Bytes(), &operation); err != nil {
			return nil, fmt.Errorf("decode storm operation: %w", err)
		}
		log.seen[operation.SampleID] = operation.ID
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan storm operation log: %w", err)
	}
	return log, nil
}

// Seen reports whether a sample fingerprint was already dispositioned.
func (l *OperationLog) Seen(sampleID string) (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	operationID, found := l.seen[sampleID]
	return operationID, found
}

// Append writes one operation. Appending a fingerprint that already exists is
// rejected: the journal refuses to record a second disposition for a sample.
func (l *OperationLog) Append(operation Operation) error {
	if operation.ID == "" || operation.SampleID == "" {
		return fmt.Errorf("storm operation and sample identity are required")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if existing, found := l.seen[operation.SampleID]; found {
		return fmt.Errorf("weather sample %s already dispositioned by operation %s", operation.SampleID, existing)
	}
	file, err := os.OpenFile(l.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open storm operation stream: %w", err)
	}
	encoded, err := json.Marshal(operation)
	if err == nil {
		_, err = file.Write(append(encoded, '\n'))
	}
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return fmt.Errorf("append storm operation: %w", err)
	}
	if closeErr != nil {
		return fmt.Errorf("close storm operation stream: %w", closeErr)
	}
	l.seen[operation.SampleID] = operation.ID
	return nil
}

// Read returns all recorded operations in journal order.
func (l *OperationLog) Read() ([]Operation, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	file, err := os.Open(l.path)
	if os.IsNotExist(err) {
		return []Operation{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open storm operation log: %w", err)
	}
	defer file.Close()
	var operations []Operation
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		var operation Operation
		if err := json.Unmarshal(scanner.Bytes(), &operation); err != nil {
			return nil, fmt.Errorf("decode storm operation: %w", err)
		}
		operations = append(operations, operation)
	}
	return operations, scanner.Err()
}
