// Command inpkg runs this module's in-package go-raknet tests and benchmarks.
//
// It copies the go.sum-verified pinned go-raknet module into a scratch directory,
// adds the inpkg/*_test.go files to its root package and runs `go test` there,
// forwarding every argument. Unexported codec and connection internals are
// reachable without forking the baseline.
package main

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "inpkg:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	_, self, _, ok := runtime.Caller(0)
	if !ok {
		return fmt.Errorf("locate source directory")
	}
	module := filepath.Clean(filepath.Join(filepath.Dir(self), "..", ".."))
	dir, err := pinnedModuleDir(module)
	if err != nil {
		return err
	}
	scratch, err := os.MkdirTemp("", "go-raknet-inpkg-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(scratch)
	if err := copyTree(dir, scratch); err != nil {
		return fmt.Errorf("copy pinned module: %w", err)
	}
	extras, err := filepath.Glob(filepath.Join(module, "inpkg", "*_test.go"))
	if err != nil {
		return err
	}
	for _, extra := range extras {
		if err := copyFile(extra, filepath.Join(scratch, filepath.Base(extra)), 0o644); err != nil {
			return err
		}
	}
	cmd := exec.Command("go", append([]string{"test", "-tags", "raknetparity"}, args...)...)
	cmd.Dir = scratch
	cmd.Stdout, cmd.Stderr, cmd.Stdin = os.Stdout, os.Stderr, os.Stdin
	// The in-package tests read and write the repository's committed fixtures.
	cmd.Env = append(os.Environ(), "RAKNET_REPO_ROOT="+filepath.Clean(filepath.Join(module, "..", "..")), "GOFLAGS=-mod=mod", "GOWORK=off")
	return cmd.Run()
}

// pinnedModuleDir downloads the replaced go-raknet module named by go.mod, verified by go.sum.
func pinnedModuleDir(module string) (string, error) {
	cmd := exec.Command("go", "mod", "download", "-json", "github.com/sandertv/go-raknet")
	cmd.Dir = module
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("download pinned go-raknet: %w", err)
	}
	var info struct{ Dir, Error string }
	if err := json.Unmarshal(out, &info); err != nil {
		return "", err
	}
	if info.Error != "" || info.Dir == "" {
		return "", fmt.Errorf("download pinned go-raknet: %s", info.Error)
	}
	return info.Dir, nil
}

func copyTree(src, dst string) error {
	return filepath.WalkDir(src, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if entry.IsDir() {
			if strings.HasPrefix(entry.Name(), ".") && rel != "." {
				return filepath.SkipDir
			}
			return os.MkdirAll(target, 0o755)
		}
		return copyFile(path, target, 0o644)
	})
}

func copyFile(src, dst string, mode fs.FileMode) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, mode)
}
