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

package schemamanager

import (
	"encoding/json"
	"fmt"
	"time"

	"context"

	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

const (
	// SchemaChangeDirName is the key name in the ControllerFactory params.
	// It specifies the schema change directory.
	SchemaChangeDirName = "schema-change-dir"
	// SchemaChangeUser is the key name in the ControllerFactory params.
	// It specifies the user who submits this schema change.
	SchemaChangeUser = "schema-change-user"
)

// ControllerFactory takes a set params and construct a Controller instance.
type ControllerFactory func(params map[string]string) (Controller, error)

var (
	controllerFactories = make(map[string]ControllerFactory)
)

// Controller is responsible for getting schema change for a
// certain keyspace and also handling various events happened during schema
// change.
type Controller interface {
	Open(ctx context.Context) error
	Read(ctx context.Context) (sqls []string, err error)
	Close()
	Keyspace() string
	OnReadSuccess(ctx context.Context) error
	OnReadFail(ctx context.Context, err error) error
	OnValidationSuccess(ctx context.Context) error
	OnValidationFail(ctx context.Context, err error) error
	OnExecutorComplete(ctx context.Context, result *ExecuteResult) error
}

// Executor applies schema changes to underlying system
type Executor interface {
	Open(ctx context.Context, keyspace string) error
	Validate(ctx context.Context, sqls []string) error
	Execute(ctx context.Context, sqls []string) *ExecuteResult
	Close()
}

// ExecuteResult contains information about schema management state
type ExecuteResult struct {
	FailedShards   []ShardWithError
	SuccessShards  []ShardResult
	CurSQLIndex    int
	Sqls           []string
	UUIDs          []string
	ExecutorErr    string
	TotalTimeSpent time.Duration
}

// ShardWithError contains information why a shard failed to execute given sql
type ShardWithError struct {
	Shard string
	Err   string
}

// ShardResult contains sql execute information on a particular shard
type ShardResult struct {
	Shard   string
	Results []*querypb.QueryResult
	// Position is a replication position that is guaranteed to be after the
	// schema change was applied. It can be used to wait for replicas to receive
	// the schema change via replication.
	Position string
}

// Run applies schema changes on Vitess through VtGate.
func Run(ctx context.Context, controller Controller, executor Executor) (execResult *ExecuteResult, err error) {
	if err := controller.Open(ctx); err != nil {
		log.Errorf("failed to open data sourcer: %v", err)
		return execResult, err
	}
	defer controller.Close()
	sqls, err := controller.Read(ctx)
	if err != nil {
		log.Errorf("failed to read data from data sourcer: %v", err)
		controller.OnReadFail(ctx, err)
		return execResult, err
	}
	controller.OnReadSuccess(ctx)
	if len(sqls) == 0 {
		return execResult, nil
	}
	keyspace := controller.Keyspace()
	if err := executor.Open(ctx, keyspace); err != nil {
		log.Errorf("failed to open executor: %v", err)
		return execResult, err
	}
	defer executor.Close()
	if err := executor.Validate(ctx, sqls); err != nil {
		log.Errorf("validation fail: %v", err)
		controller.OnValidationFail(ctx, err)
		return execResult, err
	}

	if err := controller.OnValidationSuccess(ctx); err != nil {
		return execResult, err
	}

	execResult = executor.Execute(ctx, sqls)

	if err := controller.OnExecutorComplete(ctx, execResult); err != nil {
		return execResult, err
	}
	if execResult.ExecutorErr != "" || len(execResult.FailedShards) > 0 {
		out, _ := json.MarshalIndent(execResult, "", "  ")
		return execResult, fmt.Errorf("schema change failed, ExecuteResult: %v", string(out))
	}
	return execResult, nil
}

// RegisterControllerFactory register a control factory.
func RegisterControllerFactory(name string, factory ControllerFactory) {
	if _, ok := controllerFactories[name]; ok {
		panic(fmt.Sprintf("register a registered key: %s", name))
	}
	controllerFactories[name] = factory
}

// GetControllerFactory gets a ControllerFactory.
func GetControllerFactory(name string) (ControllerFactory, error) {
	factory, ok := controllerFactories[name]
	if !ok {
		return nil, fmt.Errorf("there is no data sourcer factory with name: %s", name)
	}
	return factory, nil
}
