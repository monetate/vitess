/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package backupstorage contains the interface and file system implementation
// of the backup system.
package backupstorage

import (
	"context"
	"fmt"
	"io"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/mysqlctl/errors"
	"vitess.io/vitess/go/vt/utils"

	"vitess.io/vitess/go/vt/servenv"
)

var (
	// BackupStorageImplementation is the implementation to use
	// for BackupStorage. Exported for test purposes.
	BackupStorageImplementation string
	// FileSizeUnknown is a special value indicating that the file size is not known.
	// This is typically used while creating a file programmatically, where it is
	// impossible to compute the final size on disk ahead of time.
	FileSizeUnknown = int64(-1)
)

func registerBackupFlags(fs *pflag.FlagSet) {
	utils.SetFlagStringVar(fs, &BackupStorageImplementation, "backup-storage-implementation", "", "Which backup storage implementation to use for creating and restoring backups.")
}

func init() {
	servenv.OnParseFor("vtbackup", registerBackupFlags)
	servenv.OnParseFor("vtctl", registerBackupFlags)
	servenv.OnParseFor("vtctld", registerBackupFlags)
	servenv.OnParseFor("vttablet", registerBackupFlags)
}

// BackupHandle describes an individual backup.
type BackupHandle interface {
	// Directory is the location of the backup. Will contain keyspace/shard.
	Directory() string

	// Name is the individual name of the backup. Will contain
	// tabletAlias-timestamp.
	Name() string

	// AddFile opens a new file to be added to the backup.
	// Only works for read-write backups (created by StartBackup).
	// filename is guaranteed to only contain alphanumerical
	// characters and hyphens.
	// It should be thread safe, it is possible to call AddFile in
	// multiple go routines once a backup has been started.
	// The context is valid for the duration of the writes, until the
	// WriteCloser is closed.
	// filesize should not be treated as an exact value but rather
	// as an approximate value.
	// A filesize of -1 should be treated as a special value indicating that
	// the file size is unknown.
	AddFile(ctx context.Context, filename string, filesize int64) (io.WriteCloser, error)

	// EndBackup stops and closes a backup. The contents should be kept.
	// Only works for read-write backups (created by StartBackup).
	EndBackup(ctx context.Context) error

	// AbortBackup stops a backup, and removes the contents that
	// have been copied already. It is called if an error occurs
	// while the backup is being taken, and the backup cannot be finished.
	// Only works for read-write backups (created by StartBackup).
	AbortBackup(ctx context.Context) error

	// ReadFile starts reading a file from a backup.
	// Only works for read-only backups (created by ListBackups).
	// The context is valid for the duration of the reads, until the
	// ReadCloser is closed.
	ReadFile(ctx context.Context, filename string) (io.ReadCloser, error)

	// BackupErrorRecorder is embedded here to coordinate reporting and
	// handling of errors among all the components involved in taking/restoring a backup.
	errors.BackupErrorRecorder
}

// BackupStorage is the interface to the storage system
type BackupStorage interface {
	// ListBackups returns all the backups in a directory.  The
	// returned backups are read-only (ReadFile can be called, but
	// AddFile/EndBackup/AbortBackup cannot).
	// The backups are string-sorted by Name(), ascending (ends up
	// being the oldest backup first).
	ListBackups(ctx context.Context, dir string) ([]BackupHandle, error)

	// StartBackup creates a new backup with the given name.  If a
	// backup with the same name already exists, it's an error.
	// The returned backup is read-write
	// (AddFile/EndBackup/AbortBackup can all be called, not
	// ReadFile). The provided context is only valid for that
	// function, and should not be stored by the implementation.
	StartBackup(ctx context.Context, dir, name string) (BackupHandle, error)

	// RemoveBackup removes all the data associated with a backup.
	// It will not appear in ListBackups after RemoveBackup succeeds.
	RemoveBackup(ctx context.Context, dir, name string) error

	// Close frees resources associated with an active backup
	// session, such as closing connections. Implementations of
	// BackupStorage must support being reused after Close() is called.
	Close() error

	// WithParams should return a new BackupStorage which is a shared-nothing
	// copy of the current BackupStorage and which uses Params.
	//
	// This method is intended to give BackupStorage implementations logging
	// and metrics mechanisms.
	WithParams(Params) BackupStorage
}

// BackupStorageMap contains the registered implementations for BackupStorage
var BackupStorageMap = make(map[string]BackupStorage)

// GetBackupStorage returns the current BackupStorage implementation.
// Should be called after flags have been initialized.
// When all operations are done, call BackupStorage.Close() to free resources.
func GetBackupStorage() (BackupStorage, error) {
	bs, ok := BackupStorageMap[BackupStorageImplementation]
	if !ok {
		return nil, fmt.Errorf("no registered implementation of BackupStorage")
	}
	return bs, nil
}
