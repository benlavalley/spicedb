package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
)

const (
	colGCLocks = "gc_locks"
	gcLockID   = "gc_run_lock"

	// gcBatchDeleteSize is the number of documents to delete in each batch
	gcBatchDeleteSize = 1000
)

var (
	_ common.GarbageCollectableDatastore = (*mongoDBDatastore)(nil)
	_ common.GarbageCollector            = (*mongoGarbageCollector)(nil)
)

// gcLockDoc represents a lock document for GC coordination
type gcLockDoc struct {
	ID        string    `bson:"_id"`
	LockedBy  string    `bson:"locked_by"`
	LockedAt  time.Time `bson:"locked_at"`
	ExpiresAt time.Time `bson:"expires_at"`
}

// mongoGarbageCollector implements the common.GarbageCollector interface
type mongoGarbageCollector struct {
	ds       *mongoDBDatastore
	lockID   string // unique identifier for this GC instance
	isClosed bool
}

// BuildGarbageCollector creates a new garbage collector instance for a single GC run.
func (m *mongoDBDatastore) BuildGarbageCollector(ctx context.Context) (common.GarbageCollector, error) {
	m.RLock()
	defer m.RUnlock()

	if m.closed {
		return nil, ErrMongoDBClosed
	}

	return &mongoGarbageCollector{
		ds:       m,
		lockID:   m.uniqueID + "-" + strconv.FormatInt(time.Now().UnixNano(), 10),
		isClosed: false,
	}, nil
}

// HasGCRun returns true if garbage collection has completed at least once.
func (m *mongoDBDatastore) HasGCRun() bool {
	return m.gcHasRun.Load()
}

// MarkGCCompleted marks that garbage collection has completed.
func (m *mongoDBDatastore) MarkGCCompleted() {
	m.gcHasRun.Store(true)
}

// ResetGCCompleted resets the GC completion state.
func (m *mongoDBDatastore) ResetGCCompleted() {
	m.gcHasRun.Store(false)
}

// Close releases resources held by the garbage collector.
func (gc *mongoGarbageCollector) Close() {
	gc.isClosed = true
}

// LockForGCRun attempts to acquire a distributed lock for GC.
// Uses MongoDB's findAndModify for atomic lock acquisition with expiration.
func (gc *mongoGarbageCollector) LockForGCRun(ctx context.Context) (bool, error) {
	if gc.isClosed {
		return false, errors.New("garbage collector is closed")
	}

	col := gc.ds.database.Collection(colGCLocks)
	now := time.Now()
	// Lock expires after 5 minutes to prevent deadlocks
	expiresAt := now.Add(5 * time.Minute)

	// Try to acquire the lock by either:
	// 1. Creating it if it doesn't exist
	// 2. Taking it over if the existing lock has expired
	filter := bson.M{
		"_id": gcLockID,
		"$or": bson.A{
			bson.M{"expires_at": bson.M{"$lt": now}}, // Expired lock
		},
	}
	update := bson.M{
		"$set": bson.M{
			"locked_by":  gc.lockID,
			"locked_at":  now,
			"expires_at": expiresAt,
		},
	}

	opts := options.FindOneAndUpdate().
		SetUpsert(false).
		SetReturnDocument(options.After)

	var result gcLockDoc
	err := col.FindOneAndUpdate(ctx, filter, update, opts).Decode(&result)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			// Lock doesn't exist or isn't expired, try to insert a new one
			newLock := gcLockDoc{
				ID:        gcLockID,
				LockedBy:  gc.lockID,
				LockedAt:  now,
				ExpiresAt: expiresAt,
			}
			_, insertErr := col.InsertOne(ctx, newLock)
			if insertErr != nil {
				if mongo.IsDuplicateKeyError(insertErr) {
					// Another process acquired the lock
					return false, nil
				}
				return false, fmt.Errorf("failed to acquire GC lock: %w", insertErr)
			}
			return true, nil
		}
		return false, fmt.Errorf("failed to acquire GC lock: %w", err)
	}

	// Check if we actually acquired the lock
	return result.LockedBy == gc.lockID, nil
}

// UnlockAfterGCRun releases the GC lock.
// Uses a background context since the original context may be cancelled.
func (gc *mongoGarbageCollector) UnlockAfterGCRun() error {
	ctx := context.Background()
	col := gc.ds.database.Collection(colGCLocks)

	// Only delete if we own the lock
	result, err := col.DeleteOne(ctx, bson.M{
		"_id":       gcLockID,
		"locked_by": gc.lockID,
	})
	if err != nil {
		return fmt.Errorf("failed to release GC lock: %w", err)
	}

	if result.DeletedCount == 0 {
		// Lock was already released or taken by someone else
		return nil
	}

	return nil
}

// Now returns the current time.
func (gc *mongoGarbageCollector) Now(ctx context.Context) (time.Time, error) {
	if gc.isClosed {
		return time.Time{}, errors.New("garbage collector is closed")
	}

	return time.Now().UTC(), nil
}

// TxIDBefore returns a revision representing the watermark before the given time.
// For timestamp-based revisions, this is simply the time converted to nanoseconds.
func (gc *mongoGarbageCollector) TxIDBefore(ctx context.Context, before time.Time) (datastore.Revision, error) {
	if gc.isClosed {
		return datastore.NoRevision, errors.New("garbage collector is closed")
	}

	// For timestamp-based revisions, the revision is the time itself
	return revisions.NewForTime(before), nil
}

// DeleteBeforeTx deletes all soft-deleted data before the given revision.
func (gc *mongoGarbageCollector) DeleteBeforeTx(ctx context.Context, txID datastore.Revision) (common.DeletionCounts, error) {
	if gc.isClosed {
		return common.DeletionCounts{}, errors.New("garbage collector is closed")
	}

	rev := txID.(revisions.TimestampRevision)
	watermark := rev.TimestampNanoSec()
	counts := common.DeletionCounts{}

	// Delete soft-deleted relationships
	relCount, err := gc.batchDelete(ctx, colRelationships, bson.M{
		"deleted_revision": bson.M{
			"$gt": int64(0),  // Is soft-deleted
			"$lt": watermark, // Deleted before watermark
		},
	})
	if err != nil {
		return counts, fmt.Errorf("failed to GC relationships: %w", err)
	}
	counts.Relationships = relCount

	// Delete soft-deleted namespaces
	nsCount, err := gc.batchDelete(ctx, colNamespaces, bson.M{
		"deleted_revision": bson.M{
			"$gt": int64(0),
			"$lt": watermark,
		},
	})
	if err != nil {
		return counts, fmt.Errorf("failed to GC namespaces: %w", err)
	}
	counts.Namespaces = nsCount

	// Delete soft-deleted caveats
	caveatCount, err := gc.batchDelete(ctx, colCaveats, bson.M{
		"deleted_revision": bson.M{
			"$gt": int64(0),
			"$lt": watermark,
		},
	})
	if err != nil {
		return counts, fmt.Errorf("failed to GC caveats: %w", err)
	}
	// Caveats are counted as namespaces in the common interface
	counts.Namespaces += caveatCount

	// Delete old changelog entries
	changelogCount, err := gc.batchDelete(ctx, colChangelog, bson.M{
		"revision": bson.M{"$lt": watermark},
	})
	if err != nil {
		return counts, fmt.Errorf("failed to GC changelog: %w", err)
	}
	counts.Transactions = changelogCount

	return counts, nil
}

// DeleteExpiredRels deletes relationships that have passed their expiration time.
func (gc *mongoGarbageCollector) DeleteExpiredRels(ctx context.Context) (int64, error) {
	if gc.isClosed {
		return 0, errors.New("garbage collector is closed")
	}

	now := time.Now().UTC()
	gcCutoff := now.Add(-gc.ds.gcWindow)

	return gc.batchDelete(ctx, colRelationships, bson.M{
		"expiration": bson.M{
			"$ne": nil,      // Has expiration
			"$lt": gcCutoff, // Expired before GC window
		},
		"deleted_revision": int64(0), // Still active
	})
}

// batchDelete deletes documents matching the filter in batches.
func (gc *mongoGarbageCollector) batchDelete(ctx context.Context, collection string, filter bson.M) (int64, error) {
	col := gc.ds.database.Collection(collection)
	var totalDeleted int64

	for {
		// Find a batch of documents to delete
		cursor, err := col.Find(ctx, filter, options.Find().SetLimit(gcBatchDeleteSize).SetProjection(bson.M{"_id": 1}))
		if err != nil {
			return totalDeleted, err
		}

		var ids []any
		for cursor.Next(ctx) {
			var doc struct {
				ID any `bson:"_id"`
			}
			if err := cursor.Decode(&doc); err != nil {
				cursor.Close(ctx)
				return totalDeleted, err
			}
			ids = append(ids, doc.ID)
		}
		cursor.Close(ctx)

		if len(ids) == 0 {
			break
		}

		// Delete the batch
		result, err := col.DeleteMany(ctx, bson.M{"_id": bson.M{"$in": ids}})
		if err != nil {
			return totalDeleted, err
		}
		totalDeleted += result.DeletedCount

		// If we deleted fewer than the batch size, we're done
		if result.DeletedCount < gcBatchDeleteSize {
			break
		}
	}

	return totalDeleted, nil
}
