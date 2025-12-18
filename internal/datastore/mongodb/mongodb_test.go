package mongodb

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/authzed/spicedb/pkg/datastore"
	test "github.com/authzed/spicedb/pkg/datastore/test"
)

var testDBCounter uint64

type mongoDBTest struct {
	baseURI string
}

// cleanupDatastore wraps a datastore to drop the test database on Close
type cleanupDatastore struct {
	datastore.Datastore
	dbName  string
	baseURI string
}

func (c *cleanupDatastore) Close() error {
	// First close the underlying datastore
	if err := c.Datastore.Close(); err != nil {
		return err
	}

	// Then drop the test database to clean up
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientOpts := options.Client().ApplyURI(c.baseURI)
	client, err := mongo.Connect(clientOpts)
	if err != nil {
		return nil // Ignore cleanup errors
	}
	defer func() { _ = client.Disconnect(ctx) }()

	_ = client.Database(c.dbName).Drop(ctx) // Ignore errors during cleanup
	return nil
}

func (mdt mongoDBTest) New(revisionQuantization, gcInterval, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
	ctx := context.Background()

	// Create a unique database name for each test to avoid data conflicts
	dbNum := atomic.AddUint64(&testDBCounter, 1)
	dbName := fmt.Sprintf("spicedb_test_%d_%d", time.Now().UnixNano(), dbNum)

	// Build URI with the unique database name
	uri := fmt.Sprintf("%s/%s?directConnection=true", extractBaseURI(mdt.baseURI), dbName)

	ds, err := NewMongoDBDatastore(
		ctx,
		uri,
		GCWindow(gcWindow),
		GCInterval(gcInterval),
		RevisionQuantization(revisionQuantization),
		WatchBufferLength(watchBufferLength),
	)
	if err != nil {
		return nil, err
	}

	// Wrap with cleanup logic to drop database on Close
	return &cleanupDatastore{
		Datastore: ds,
		dbName:    dbName,
		baseURI:   mdt.baseURI,
	}, nil
}

// extractBaseURI extracts the base URI without database name
func extractBaseURI(uri string) string {
	// Simple extraction - find last / before ? and take everything before it
	for i := len(uri) - 1; i >= 0; i-- {
		if uri[i] == '?' {
			// Find the / before ?
			for j := i - 1; j >= 0; j-- {
				if uri[j] == '/' {
					return uri[:j]
				}
			}
		}
		if uri[i] == '/' && i > 10 { // Skip the :// part
			return uri[:i]
		}
	}
	return uri
}

func TestMongoDBDatastore(t *testing.T) {
	// Skip if no MongoDB URI is provided
	uri := os.Getenv("SPICEDB_MONGODB_URI")
	if uri == "" {
		t.Skip("SPICEDB_MONGODB_URI not set, skipping MongoDB tests")
	}

	tester := mongoDBTest{baseURI: uri}

	// Run all tests including watch checkpoints (continuous checkpointing is now supported)
	t.Run("AllTests", func(t *testing.T) {
		test.All(t, tester, false) // Run serially for MongoDB to avoid transaction conflicts
	})

	// Run GC tests
	t.Run("GCTests", func(t *testing.T) {
		test.OnlyGCTests(t, tester, false) // Run serially
	})
}

func TestMongoDBConnection(t *testing.T) {
	uri := os.Getenv("SPICEDB_MONGODB_URI")
	if uri == "" {
		t.Skip("SPICEDB_MONGODB_URI not set, skipping MongoDB tests")
	}

	ctx := context.Background()
	ds, err := NewMongoDBDatastore(ctx, uri)
	require.NoError(t, err)
	defer ds.Close()

	// Test basic operations
	state, err := ds.ReadyState(ctx)
	require.NoError(t, err)
	require.True(t, state.IsReady)

	// Test head revision
	rev, err := ds.HeadRevision(ctx)
	require.NoError(t, err)
	require.NotNil(t, rev)

	// Test unique ID
	id, err := ds.UniqueID(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, id)

	t.Logf("MongoDB connection test passed. UniqueID: %s", id)
}
