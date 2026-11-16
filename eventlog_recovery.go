package uvim

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
)

// A newline is the append framing boundary. Startup repairs only an
// unterminated tail; malformed complete records are still corruption errors.
// The owner has not been exposed to readers or providers at this point.
func (l *EventLog) recoverUnterminatedTail() error {
	file, err := os.Open(l.Path)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("EventLog must be a regular file")
	}
	if info.Size() == 0 {
		return nil
	}
	last := []byte{0}
	if _, err = file.ReadAt(last, info.Size()-1); err != nil {
		return err
	}
	if last[0] == '\n' {
		return nil
	}
	committed := int64(0)
	buffer := make([]byte, 64<<10)
	for end := info.Size(); end > 0; {
		start := end - int64(len(buffer))
		if start < 0 {
			start = 0
		}
		n, readErr := file.ReadAt(buffer[:end-start], start)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return readErr
		}
		if index := bytes.LastIndexByte(buffer[:n], '\n'); index >= 0 {
			committed = start + int64(index) + 1
			break
		}
		end = start
	}
	writer, err := os.OpenFile(l.Path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer writer.Close()
	current, err := writer.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(info, current) || current.Size() != info.Size() {
		return errors.New("EventLog changed during tail recovery")
	}
	if err = writer.Truncate(committed); err != nil {
		return err
	}
	if err = writer.Sync(); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(l.Path))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
