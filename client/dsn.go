package main

import (
	"bufio"
	"os"
	"strings"
)

func readDSN(filename string) (string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer f.Close()

	sb := new(strings.Builder)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		txt := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(txt, "#") {
			continue
		}
		sb.WriteString(txt)
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	return sb.String(), nil
}
