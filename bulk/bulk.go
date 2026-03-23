/*
 * Copyright 2025 Greptime Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bulk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	gpbv1 "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"
	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
)

// BulkClient is a client for performing bulk operations with GreptimeDB.
type BulkClient struct {
	client flight.FlightServiceClient
}

// NewBulkClient creates a new BulkClient instance using the provided gRPC connection.
func NewBulkClient(conn *grpc.ClientConn) *BulkClient {
	return &BulkClient{
		client: flight.NewFlightServiceClient(conn),
	}
}

type BulkWriter interface {
	Write(table *table.Table) error
	Close() error
}

type bulkWriter struct {
	client *BulkClient
	stream flight.FlightService_DoPutClient
	writer *flight.Writer

	recvDone chan struct{} // closed when the recv goroutine exits
	mu       sync.RWMutex
	recvErr  error  // first non-EOF error from recv goroutine
	rows     uint32 // total affected rows reported by server
}

// NewBulkWriter creates a new BulkWriter instance for streaming data to GreptimeDB.
func (c *BulkClient) NewBulkWriter(ctx context.Context) (BulkWriter, error) {
	stream, err := c.client.DoPut(ctx)
	if err != nil {
		return nil, err
	}

	bw := &bulkWriter{
		client:   c,
		stream:   stream,
		recvDone: make(chan struct{}),
	}
	go bw.recvLoop()
	return bw, nil
}

// recvLoop drains all PutResult messages from the server in the background,
// accumulating affected rows and capturing the first error.
func (bw *bulkWriter) recvLoop() {
	defer close(bw.recvDone)
	for {
		resp, err := bw.stream.Recv()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				bw.mu.Lock()
				bw.recvErr = err
				bw.mu.Unlock()
			}
			return
		}
		if resp != nil && len(resp.AppMetadata) > 0 {
			var meta gpbv1.FlightMetadata
			if unmarshalErr := proto.Unmarshal(resp.AppMetadata, &meta); unmarshalErr == nil {
				bw.mu.Lock()
				bw.rows += meta.GetAffectedRows().GetValue()
				bw.mu.Unlock()
			}
		}
	}
}

// Write converts and sends a table of data to GreptimeDB in Arrow format.
func (bw *bulkWriter) Write(tb *table.Table) error {
	bw.mu.RLock()
	err := bw.recvErr
	bw.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("stream already failed: %w", err)
	}

	converter := types.NewArrowConverter()
	tableName, err := tb.GetName()
	if err != nil {
		return err
	}

	record, err := converter.ToArrow(tb.GetRows())
	if err != nil {
		return fmt.Errorf("failed to convert to Arrow: %w", err)
	}
	defer record.Release()

	if bw.writer == nil {
		descriptor := &flight.FlightDescriptor{
			Type: flight.DescriptorPATH,
			Path: []string{tableName},
		}

		bw.writer = flight.NewRecordWriter(bw.stream, ipc.WithSchema(record.Schema()))
		bw.writer.SetFlightDescriptor(descriptor)
	}

	return bw.writer.Write(record)
}

// Close finalizes the bulk write operation and cleans up resources.
// This must be called after all writes are completed.
func (bw *bulkWriter) Close() error {
	var errs []error

	if bw.writer != nil {
		errs = append(errs, bw.writer.Close())
		bw.writer = nil
	}

	if bw.stream != nil {
		errs = append(errs, bw.stream.CloseSend())
		// Wait for recvLoop to drain all server responses (including the
		// final result sent after the background write task completes).
		<-bw.recvDone
		if bw.recvErr != nil {
			errs = append(errs, bw.recvErr)
		}
		bw.stream = nil
	}

	return errors.Join(errs...)
}

// BulkWrite performs a single bulk write operation with the given table data.
func (c *BulkClient) BulkWrite(ctx context.Context, tb *table.Table) (*gpbv1.GreptimeResponse, error) {
	bw, err := c.NewBulkWriter(ctx)
	if err != nil {
		return nil, err
	}

	if err := bw.Write(tb); err != nil {
		return nil, errors.Join(err, bw.Close())
	}

	if err := bw.Close(); err != nil {
		return nil, err
	}

	writer := bw.(*bulkWriter)
	return &gpbv1.GreptimeResponse{
		Response: &gpbv1.GreptimeResponse_AffectedRows{
			AffectedRows: &gpbv1.AffectedRows{
				Value: writer.rows,
			},
		},
	}, nil
}
