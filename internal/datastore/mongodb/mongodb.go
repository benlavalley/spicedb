package mongodb

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	dsoptions "github.com/authzed/spicedb/pkg/datastore/options"
)

const (
	Engine                   = "mongodb"
	defaultWatchBufferLength = 128
	defaultGCWindow          = 24 * time.Hour
	defaultQuantization      = 5 * time.Second
)

func init() {
	datastore.Engines = append(datastore.Engines, Engine)
}

var ErrMongoDBClosed = errors.New("mongodb datastore is closed")

// mongoDBDatastore implements the datastore.Datastore interface for MongoDB.
type mongoDBDatastore struct {
	sync.RWMutex
	revisions.CommonDecoder

	client   *mongo.Client
	database *mongo.Database

	// Configuration
	gcWindow                    time.Duration
	gcInterval                  time.Duration
	gcMaxOperationTime          time.Duration
	gcEnabled                   bool
	revisionQuantization        time.Duration
	maxRevisionStalenessPercent float64
	watchBufferLength           uint16
	watchBufferWriteTimeout     time.Duration

	// State
	uniqueID string
	closed   bool // GUARDED_BY(RWMutex)
	gcHasRun atomic.Bool

	// GC cancellation
	gcCtx    context.Context
	gcCancel context.CancelFunc
}

// NewMongoDBDatastore creates a new MongoDB-backed datastore.
func NewMongoDBDatastore(ctx context.Context, uri string, opts ...Option) (datastore.Datastore, error) {
	// Create MongoDB client
	clientOpts := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(clientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Ping to verify connection
	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	// Extract database name from URI or use default
	dbName := extractDatabaseFromURI(uri)
	if dbName == "" {
		dbName = "spicedb"
	}

	db := client.Database(dbName)

	ds := &mongoDBDatastore{
		CommonDecoder: revisions.CommonDecoder{
			Kind: revisions.Timestamp,
		},
		client:                      client,
		database:                    db,
		gcWindow:                    defaultGCWindow,
		gcInterval:                  defaultGCInterval,
		gcMaxOperationTime:          defaultGCMaxOperationTime,
		gcEnabled:                   true,
		revisionQuantization:        defaultQuantization,
		maxRevisionStalenessPercent: 0.1,
		watchBufferLength:           defaultWatchBufferLength,
		watchBufferWriteTimeout:     100 * time.Millisecond,
	}

	// Apply options
	for _, opt := range opts {
		opt(ds)
	}

	// Initialize indexes
	if err := ds.createIndexes(ctx); err != nil {
		return nil, fmt.Errorf("failed to create indexes: %w", err)
	}

	// Get or create unique ID
	uniqueID, err := ds.getOrCreateUniqueID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get unique ID: %w", err)
	}
	ds.uniqueID = uniqueID

	// Start GC worker if enabled
	if ds.gcInterval > 0 && ds.gcEnabled {
		ds.gcCtx, ds.gcCancel = context.WithCancel(context.Background())
		go func() {
			// GC errors are logged internally by common.StartGarbageCollector
			// and it uses exponential backoff, so we just ignore the final error
			_ = common.StartGarbageCollector(
				ds.gcCtx,
				ds,
				ds.gcInterval,
				ds.gcWindow,
				ds.gcMaxOperationTime,
			)
		}()
	}

	return ds, nil
}

// createIndexes creates the necessary indexes for the datastore.
func (m *mongoDBDatastore) createIndexes(ctx context.Context) error {
	// Relationships collection indexes
	relCol := m.database.Collection(colRelationships)
	_, err := relCol.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "namespace", Value: 1},
				{Key: "resource_id", Value: 1},
				{Key: "relation", Value: 1},
				{Key: "subject_namespace", Value: 1},
				{Key: "subject_object_id", Value: 1},
				{Key: "subject_relation", Value: 1},
				{Key: "deleted_revision", Value: 1},
			},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{
				{Key: "namespace", Value: 1},
				{Key: "deleted_revision", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "namespace", Value: 1},
				{Key: "relation", Value: 1},
				{Key: "deleted_revision", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "subject_namespace", Value: 1},
				{Key: "deleted_revision", Value: 1},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create relationship indexes: %w", err)
	}

	// Changelog collection index
	changelogCol := m.database.Collection(colChangelog)
	_, err = changelogCol.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "revision", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return fmt.Errorf("failed to create changelog index: %w", err)
	}

	return nil
}

// getOrCreateUniqueID gets or creates a unique ID for the datastore.
func (m *mongoDBDatastore) getOrCreateUniqueID(ctx context.Context) (string, error) {
	metaCol := m.database.Collection(colMetadata)

	var doc metadataDoc
	err := metaCol.FindOne(ctx, bson.M{"_id": uniqueIDKey}).Decode(&doc)
	if err == nil {
		return doc.Value, nil
	}

	if !errors.Is(err, mongo.ErrNoDocuments) {
		return "", fmt.Errorf("failed to find unique ID: %w", err)
	}

	// Create new unique ID
	newID := uuid.NewString()
	_, err = metaCol.InsertOne(ctx, metadataDoc{
		Key:   uniqueIDKey,
		Value: newID,
	})
	if err != nil {
		// Check if another instance created it
		if mongo.IsDuplicateKeyError(err) {
			err = metaCol.FindOne(ctx, bson.M{"_id": uniqueIDKey}).Decode(&doc)
			if err == nil {
				return doc.Value, nil
			}
		}
		return "", fmt.Errorf("failed to create unique ID: %w", err)
	}

	return newID, nil
}

// MetricsID returns an identifier for the datastore for use in metrics.
func (m *mongoDBDatastore) MetricsID() (string, error) {
	return "mongodb", nil
}

// UniqueID returns a unique identifier for the datastore.
func (m *mongoDBDatastore) UniqueID(_ context.Context) (string, error) {
	return m.uniqueID, nil
}

// SnapshotReader creates a read-only handle that reads the datastore at the specified revision.
func (m *mongoDBDatastore) SnapshotReader(rev datastore.Revision) datastore.Reader {
	m.RLock()
	defer m.RUnlock()

	if m.closed {
		return &mongoDBReader{
			database: nil,
			revision: rev.(revisions.TimestampRevision),
			initErr:  ErrMongoDBClosed,
		}
	}

	return &mongoDBReader{
		database: m.database,
		revision: rev.(revisions.TimestampRevision),
		now:      time.Now(),
	}
}

// nowRevision returns a new revision based on the current time.
func nowRevision() revisions.TimestampRevision {
	return revisions.NewForTime(time.Now())
}

// OptimizedRevision gets a revision that will likely already be replicated.
func (m *mongoDBDatastore) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	m.RLock()
	defer m.RUnlock()

	if m.closed {
		return datastore.NoRevision, ErrMongoDBClosed
	}

	now := time.Now()
	quantized := now.Add(-m.revisionQuantization)
	return revisions.NewForTime(quantized), nil
}

// HeadRevision gets a revision that is guaranteed to be at least as fresh as right now.
func (m *mongoDBDatastore) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	m.RLock()
	defer m.RUnlock()

	if m.closed {
		return datastore.NoRevision, ErrMongoDBClosed
	}

	return nowRevision(), nil
}

// CheckRevision checks the specified revision to make sure it's valid.
func (m *mongoDBDatastore) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	m.RLock()
	defer m.RUnlock()

	if m.closed {
		return ErrMongoDBClosed
	}

	// Handle nil/NoRevision
	if revision == nil || revision == datastore.NoRevision {
		return datastore.NewInvalidRevisionErr(revision, datastore.CouldNotDetermineRevision)
	}

	tsRev, ok := revision.(revisions.TimestampRevision)
	if !ok {
		return datastore.NewInvalidRevisionErr(revision, datastore.CouldNotDetermineRevision)
	}

	revTime := tsRev.Time()
	gcCutoff := time.Now().Add(-m.gcWindow)

	if revTime.Before(gcCutoff) {
		return datastore.NewInvalidRevisionErr(revision, datastore.RevisionStale)
	}

	return nil
}

// RevisionFromString parses a revision string.
func (m *mongoDBDatastore) RevisionFromString(serialized string) (datastore.Revision, error) {
	return m.CommonDecoder.RevisionFromString(serialized)
}

// ReadWriteTx starts a read/write transaction.
func (m *mongoDBDatastore) ReadWriteTx(
	ctx context.Context,
	f datastore.TxUserFunc,
	opts ...dsoptions.RWTOptionsOption,
) (datastore.Revision, error) {
	m.Lock()
	defer m.Unlock()

	if m.closed {
		return datastore.NoRevision, ErrMongoDBClosed
	}

	config := dsoptions.NewRWTOptionsWithOptions(opts...)

	// Start a session for the transaction
	session, err := m.client.StartSession()
	if err != nil {
		return datastore.NoRevision, fmt.Errorf("failed to start session: %w", err)
	}
	defer session.EndSession(ctx)

	newRevision := nowRevision()

	// Run the transaction
	_, err = session.WithTransaction(ctx, func(sc context.Context) (any, error) {
		rwt := &mongoDBReadWriteTx{
			mongoDBReader: mongoDBReader{
				database: m.database,
				revision: newRevision,
				now:      time.Now(),
			},
			session:     session,
			newRevision: newRevision,
			config:      config,
		}

		// Run the user's function
		if err := f(sc, rwt); err != nil {
			return nil, err
		}

		// Write changelog entry before commit
		if err := rwt.writeChangelog(sc); err != nil {
			return nil, err
		}

		return nil, nil
	})
	if err != nil {
		return datastore.NoRevision, err
	}

	return newRevision, nil
}

// ReadyState returns a state indicating whether the datastore is ready to accept data.
func (m *mongoDBDatastore) ReadyState(ctx context.Context) (datastore.ReadyState, error) {
	m.RLock()
	defer m.RUnlock()

	if m.closed {
		return datastore.ReadyState{
			Message: "datastore is closed",
			IsReady: false,
		}, nil
	}

	// Ping to check connection
	if err := m.client.Ping(ctx, nil); err != nil {
		return datastore.ReadyState{
			Message: fmt.Sprintf("failed to ping MongoDB: %v", err),
			IsReady: false,
		}, nil
	}

	return datastore.ReadyState{
		Message: "ready",
		IsReady: true,
	}, nil
}

// OfflineFeatures returns features supported without database connection.
func (m *mongoDBDatastore) OfflineFeatures() (*datastore.Features, error) {
	return &datastore.Features{
		Watch: datastore.Feature{
			Status: datastore.FeatureSupported,
		},
		IntegrityData: datastore.Feature{
			Status: datastore.FeatureSupported,
		},
		ContinuousCheckpointing: datastore.Feature{
			Status: datastore.FeatureSupported,
		},
		WatchEmitsImmediately: datastore.Feature{
			Status: datastore.FeatureUnsupported,
		},
	}, nil
}

// Features returns features supported by the datastore.
func (m *mongoDBDatastore) Features(ctx context.Context) (*datastore.Features, error) {
	return m.OfflineFeatures()
}

// Statistics returns statistics about the datastore.
func (m *mongoDBDatastore) Statistics(ctx context.Context) (datastore.Stats, error) {
	m.RLock()
	defer m.RUnlock()

	if m.closed {
		return datastore.Stats{}, ErrMongoDBClosed
	}

	// Get estimated relationship count
	relCol := m.database.Collection(colRelationships)
	count, err := relCol.EstimatedDocumentCount(ctx)
	if err != nil {
		return datastore.Stats{}, fmt.Errorf("failed to get document count: %w", err)
	}

	// Get namespace statistics
	nsCol := m.database.Collection(colNamespaces)
	cursor, err := nsCol.Find(ctx, bson.M{})
	if err != nil {
		return datastore.Stats{}, fmt.Errorf("failed to list namespaces: %w", err)
	}
	defer cursor.Close(ctx)

	var objectTypeStats []datastore.ObjectTypeStat
	for cursor.Next(ctx) {
		// For simplicity, we don't parse the namespace definition here
		// In a full implementation, we would parse and count relations/permissions
		objectTypeStats = append(objectTypeStats, datastore.ObjectTypeStat{
			NumRelations:   0,
			NumPermissions: 0,
		})
	}

	return datastore.Stats{
		UniqueID:                   m.uniqueID,
		EstimatedRelationshipCount: uint64(count),
		ObjectTypeStatistics:       objectTypeStats,
	}, nil
}

// Close closes the datastore.
func (m *mongoDBDatastore) Close() error {
	m.Lock()
	defer m.Unlock()

	if m.closed {
		return nil
	}

	m.closed = true

	// Cancel GC worker
	if m.gcCancel != nil {
		m.gcCancel()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return m.client.Disconnect(ctx)
}

// SupportsIntegrity returns whether the datastore supports integrity data.
func (m *mongoDBDatastore) SupportsIntegrity() bool {
	return true
}

// extractDatabaseFromURI extracts the database name from a MongoDB URI.
// MongoDB URI format: mongodb://[user:pass@]host[:port]/database[?options]
func extractDatabaseFromURI(uri string) string {
	parsed, err := url.Parse(uri)
	if err != nil {
		return ""
	}

	// The database is in the path, strip leading slash
	path := strings.TrimPrefix(parsed.Path, "/")

	// Remove any trailing options marker
	if idx := strings.Index(path, "?"); idx != -1 {
		path = path[:idx]
	}

	return path
}

var _ datastore.Datastore = &mongoDBDatastore{}
