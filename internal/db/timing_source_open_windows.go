package db

import "os"

func openTimingSourceFile(path string) (*os.File, error) {
	return os.Open(path)
}
