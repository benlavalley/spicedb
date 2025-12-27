package mongodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mongooptions "go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	// minimumCheckpointInterval is the minimum allowed checkpoint interval
	minimumCheckpointInterval = 100 * time.Millisecond
)

// errWatchDisconnected is a sentinel error indicating sendChange failed due to buffer timeout.
// This signals the outer watch loop to exit (the actual error was already sent to the errs channel).
var errWatchDisconnected = errors.New("watch disconnected")

// Watch notifies the caller about changes to the datastore.
func (m *mongoDBDatastore) Watch(ctx context.Context, ar datastore.Revision, options datastore.WatchOptions) (<-chan datastore.RevisionChanges, <-chan error) {
	watchBufferLength := options.WatchBufferLength
	if watchBufferLength == 0 {
		watchBufferLength = m.watchBufferLength
	}

	updates := make(chan datastore.RevisionChanges, watchBufferLength)
	errs := make(chan error, 1)

	if options.EmissionStrategy == datastore.EmitImmediatelyStrategy {
		close(updates)
		errs <- errors.New("emit immediately strategy is unsupported in MongoDB")
		return updates, errs
	}

	watchBufferWriteTimeout := options.WatchBufferWriteTimeout
	if watchBufferWriteTimeout == 0 {
		watchBufferWriteTimeout = m.watchBufferWriteTimeout
	}

	sendChange := func(change datastore.RevisionChanges) bool {
		select {
		case updates <- change:
			return true
		default:
			// If we cannot immediately write, setup the timer and try again.
		}

		timer := time.NewTimer(watchBufferWriteTimeout)
		defer timer.Stop()

		select {
		case updates <- change:
			return true
		case <-timer.C:
			errs <- datastore.NewWatchDisconnectedErr()
			return false
		}
	}

	go func() {
		defer close(updates)
		defer close(errs)

		afterRevision := ar.(revisions.TimestampRevision)

		for {
			err := m.watchChanges(ctx, afterRevision, options, sendChange, &afterRevision)
			if err != nil {
				switch {
				case errors.Is(err, errWatchDisconnected):
					// sendChange already sent the error; just exit
					return
				case errors.Is(err, context.Canceled):
					errs <- datastore.NewWatchCanceledErr()
				default:
					errs <- err
				}
				return
			}
		}
	}()

	return updates, errs
}

// watchChanges watches for changes using MongoDB Change Streams on the changelog collection.
// It implements continuous checkpointing by emitting checkpoint events at regular intervals
// based on the change stream's operation time, similar to CockroachDB's resolved timestamps.
func (m *mongoDBDatastore) watchChanges(
	ctx context.Context,
	afterRevision revisions.TimestampRevision,
	options datastore.WatchOptions,
	sendChange func(datastore.RevisionChanges) bool,
	lastRevision *revisions.TimestampRevision,
) error {
	// First, load any existing changelog entries we might have missed
	changes, newLastRevision, err := m.loadChangelogEntries(ctx, afterRevision, options)
	if err != nil {
		return err
	}

	// Send any buffered changes
	for _, change := range changes {
		if !sendChange(change) {
			return errWatchDisconnected
		}
	}
	*lastRevision = newLastRevision

	// Now set up Change Stream to watch for new changelog entries
	col := m.database.Collection(colChangelog)

	// Watch for insert operations only (changelog entries are only inserted, never updated)
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{
			"operationType": "insert",
		}}},
	}

	// Configure change stream options
	// We use FullDocument to get the complete document in the change event.
	// Note: We don't use startAtOperationTime here because:
	// 1. The loadChangelogEntries catch-up query handles historical events
	// 2. Change streams work better starting from "now" for real-time events
	// 3. startAtOperationTime can cause issues if the timestamp is before collection creation
	csOpts := mongooptions.ChangeStream().SetFullDocument(mongooptions.Required)

	changeStream, err := col.Watch(ctx, pipeline, csOpts)
	if err != nil {
		return fmt.Errorf("failed to create change stream: %w", err)
	}
	defer changeStream.Close(ctx)

	// Determine checkpoint interval for continuous checkpointing
	checkpointInterval := options.CheckpointInterval
	if checkpointInterval < minimumCheckpointInterval {
		checkpointInterval = minimumCheckpointInterval
	}

	requestedCheckpoints := options.Content&datastore.WatchCheckpoints == datastore.WatchCheckpoints
	lastCheckpointTime := time.Now()

	// Track the confirmed revision - the highest revision we've actually seen from the change stream.
	// This is used for periodic checkpoints to ensure we don't emit checkpoints with revisions
	// that are ahead of what we've actually confirmed from the database. This is similar to how
	// CockroachDB uses "resolved" timestamps and Spanner uses heartbeat timestamps.
	confirmedRevision := *lastRevision

	// Create a change tracker for deduplication and byte-size tracking.
	// This matches the pattern used by CockroachDB and PostgreSQL datastores.
	tracked := common.NewChanges(revisions.TimestampIDKeyFunc, options.Content, options.MaximumBufferedChangesByteSize)

	// Process change stream events with periodic checkpoint emission
	for {
		// Check for context cancellation first
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Use TryNext with a short timeout to allow periodic checkpoint emission
		// This is similar to how CockroachDB/Spanner use resolved/heartbeat intervals
		tryCtx, cancel := context.WithTimeout(ctx, checkpointInterval)
		hasNext := changeStream.TryNext(tryCtx)
		cancel()

		if hasNext {
			// Decode the change event
			var event struct {
				FullDocument  changelogDoc   `bson:"fullDocument"`
				ClusterTime   bson.Timestamp `bson:"clusterTime"`
				OperationTime bson.Timestamp `bson:"operationTime,omitempty"`
				WallTime      time.Time      `bson:"wallTime,omitempty"`
			}
			if err := changeStream.Decode(&event); err != nil {
				return fmt.Errorf("failed to decode change stream event: %w", err)
			}

			doc := event.FullDocument
			revision := revisions.NewForTimestamp(doc.Revision)

			// Skip if we've already seen this revision (could happen during catch-up)
			if !revision.GreaterThan(*lastRevision) {
				continue
			}

			// Add changes to the tracker for deduplication and byte-size tracking.
			// This matches the pattern used by CockroachDB and PostgreSQL.
			if err := addChangelogDocToTracker(ctx, tracked, &doc, revision); err != nil {
				return err
			}

			*lastRevision = revision
			confirmedRevision = revision

			// Extract deduplicated changes and send them, then emit checkpoint if requested.
			// FilterAndRemoveRevisionChanges returns changes where revision < bound, so we need to
			// pass a bound that's greater than the current revision to include it.
			// Adding 1 nanosecond ensures we include the current revision.
			boundRevision := revisions.NewForTimestamp(revision.TimestampNanoSec() + 1)
			filtered, err := tracked.FilterAndRemoveRevisionChanges(revisions.TimestampIDKeyLessThanFunc, boundRevision)
			if err != nil {
				return err
			}

			for _, change := range filtered {
				filteredChange := filterChangeByContent(change, options.Content)
				if filteredChange != nil {
					if !sendChange(*filteredChange) {
						return errWatchDisconnected
					}
				}
			}

			// Send checkpoint after data change if requested
			if requestedCheckpoints {
				if !sendChange(datastore.RevisionChanges{
					Revision:     revision,
					IsCheckpoint: true,
				}) {
					return errWatchDisconnected
				}
				lastCheckpointTime = time.Now()
			}
		} else {
			// No data change available - check for errors
			if err := changeStream.Err(); err != nil {
				if errors.Is(err, context.Canceled) {
					return ctx.Err()
				}
				// Don't treat timeout as error - it's expected for checkpoint emission
				if !errors.Is(err, context.DeadlineExceeded) {
					return fmt.Errorf("change stream error: %w", err)
				}
			}

			// Emit periodic checkpoint even without data changes.
			// This provides continuous checkpointing similar to CockroachDB's resolved timestamps.
			// We use the confirmed revision (last seen from change stream) rather than current time
			// to ensure we don't emit checkpoints ahead of actual database state.
			if requestedCheckpoints && time.Since(lastCheckpointTime) >= checkpointInterval {
				if !sendChange(datastore.RevisionChanges{
					Revision:     confirmedRevision,
					IsCheckpoint: true,
				}) {
					return errWatchDisconnected
				}
				lastCheckpointTime = time.Now()
			}
		}
	}
}

// filterChangeByContent filters a change based on the watch content flags.
// Returns nil if there's nothing to emit after filtering.
func filterChangeByContent(change datastore.RevisionChanges, content datastore.WatchContent) *datastore.RevisionChanges {
	filtered := datastore.RevisionChanges{
		Revision:     change.Revision,
		IsCheckpoint: change.IsCheckpoint,
		Metadatas:    change.Metadatas,
	}

	hasContent := false

	// Include relationship changes if requested
	if content&datastore.WatchRelationships == datastore.WatchRelationships && len(change.RelationshipChanges) > 0 {
		filtered.RelationshipChanges = change.RelationshipChanges
		hasContent = true
	}

	// Include schema changes if requested
	if content&datastore.WatchSchema == datastore.WatchSchema {
		if len(change.ChangedDefinitions) > 0 {
			filtered.ChangedDefinitions = change.ChangedDefinitions
			hasContent = true
		}
		if len(change.DeletedNamespaces) > 0 {
			filtered.DeletedNamespaces = change.DeletedNamespaces
			hasContent = true
		}
		if len(change.DeletedCaveats) > 0 {
			filtered.DeletedCaveats = change.DeletedCaveats
			hasContent = true
		}
	}

	if !hasContent {
		return nil
	}

	return &filtered
}

// loadChangelogEntries loads changelog entries from the database.
// This is used for catch-up when starting a watch to load any changes that occurred
// between the afterRevision and now.
func (m *mongoDBDatastore) loadChangelogEntries(
	ctx context.Context,
	afterRevision revisions.TimestampRevision,
	options datastore.WatchOptions,
) ([]datastore.RevisionChanges, revisions.TimestampRevision, error) {
	col := m.database.Collection(colChangelog)

	afterNanos := afterRevision.TimestampNanoSec()

	cursor, err := col.Find(ctx,
		bson.M{"revision": bson.M{"$gt": afterNanos}},
		mongooptions.Find().SetSort(bson.D{{Key: "revision", Value: 1}}),
	)
	if err != nil {
		return nil, afterRevision, err
	}
	defer cursor.Close(ctx)

	var changes []datastore.RevisionChanges
	lastRevision := afterRevision
	requestedCheckpoints := options.Content&datastore.WatchCheckpoints == datastore.WatchCheckpoints

	for cursor.Next(ctx) {
		var doc changelogDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, afterRevision, err
		}

		revision := revisions.NewForTimestamp(doc.Revision)
		change, err := convertChangelogDoc(&doc, revision)
		if err != nil {
			return nil, afterRevision, err
		}

		// Filter by content type
		filteredChange := filterChangeByContent(change, options.Content)
		if filteredChange != nil {
			changes = append(changes, *filteredChange)

			// Add checkpoint after each data change if requested
			if requestedCheckpoints {
				changes = append(changes, datastore.RevisionChanges{
					Revision:     revision,
					IsCheckpoint: true,
				})
			}
		}

		lastRevision = revision
	}

	return changes, lastRevision, cursor.Err()
}

// convertChangelogDoc converts a BSON changelog document to datastore.RevisionChanges.
// Note: CREATE operations are normalized to TOUCH for Watch API consistency with other datastores.
func convertChangelogDoc(doc *changelogDoc, revision revisions.TimestampRevision) (datastore.RevisionChanges, error) {
	relChanges := make([]tuple.RelationshipUpdate, 0, len(doc.Changes.RelationshipChanges))
	for _, relDoc := range doc.Changes.RelationshipChanges {
		update, err := relDoc.ToRelationshipUpdate()
		if err != nil {
			return datastore.RevisionChanges{}, err
		}
		// Normalize CREATE to TOUCH for Watch API consistency
		if update.Operation == tuple.UpdateOperationCreate {
			update.Operation = tuple.UpdateOperationTouch
		}
		relChanges = append(relChanges, update)
	}

	// Convert namespace changes
	var changedDefinitions []datastore.SchemaDefinition
	for _, nsDoc := range doc.Changes.ChangedNamespaces {
		if len(nsDoc.Definition) > 0 {
			ns := &core.NamespaceDefinition{}
			if err := ns.UnmarshalVT(nsDoc.Definition); err != nil {
				return datastore.RevisionChanges{}, fmt.Errorf("failed to unmarshal namespace definition: %w", err)
			}
			changedDefinitions = append(changedDefinitions, ns)
		}
	}

	// Convert caveat changes
	for _, caveatDoc := range doc.Changes.ChangedCaveats {
		if len(caveatDoc.Definition) > 0 {
			caveat := &core.CaveatDefinition{}
			if err := caveat.UnmarshalVT(caveatDoc.Definition); err != nil {
				return datastore.RevisionChanges{}, fmt.Errorf("failed to unmarshal caveat definition: %w", err)
			}
			changedDefinitions = append(changedDefinitions, caveat)
		}
	}

	result := datastore.RevisionChanges{
		Revision:            revision,
		RelationshipChanges: relChanges,
		ChangedDefinitions:  changedDefinitions,
		DeletedNamespaces:   doc.Changes.DeletedNamespaces,
		DeletedCaveats:      doc.Changes.DeletedCaveats,
		IsCheckpoint:        doc.Changes.IsCheckpoint,
	}

	// Convert metadata if present
	if len(doc.Changes.Metadata) > 0 {
		metadataMap := make(map[string]any)
		for k, v := range doc.Changes.Metadata {
			metadataMap[k] = v
		}
		metadata, err := structpb.NewStruct(metadataMap)
		if err != nil {
			return datastore.RevisionChanges{}, err
		}
		result.Metadatas = []*structpb.Struct{metadata}
	}

	return result, nil
}

// addChangelogDocToTracker adds changes from a changelog document to the change tracker.
// This enables deduplication and byte-size tracking matching the pattern used by
// CockroachDB and PostgreSQL datastores.
func addChangelogDocToTracker(
	ctx context.Context,
	tracked *common.Changes[revisions.TimestampRevision, int64],
	doc *changelogDoc,
	revision revisions.TimestampRevision,
) error {
	// Add relationship changes
	for _, relDoc := range doc.Changes.RelationshipChanges {
		update, err := relDoc.ToRelationshipUpdate()
		if err != nil {
			return fmt.Errorf("failed to convert relationship update: %w", err)
		}

		// Normalize CREATE to TOUCH for Watch API consistency
		op := update.Operation
		if op == tuple.UpdateOperationCreate {
			op = tuple.UpdateOperationTouch
		}

		if err := tracked.AddRelationshipChange(ctx, revision, update.Relationship, op); err != nil {
			return err
		}
	}

	// Add namespace changes
	for _, nsDoc := range doc.Changes.ChangedNamespaces {
		if len(nsDoc.Definition) > 0 {
			ns := &core.NamespaceDefinition{}
			if err := ns.UnmarshalVT(nsDoc.Definition); err != nil {
				return fmt.Errorf("failed to unmarshal namespace definition: %w", err)
			}
			if err := tracked.AddChangedDefinition(ctx, revision, ns); err != nil {
				return err
			}
		}
	}

	// Add caveat changes
	for _, caveatDoc := range doc.Changes.ChangedCaveats {
		if len(caveatDoc.Definition) > 0 {
			caveat := &core.CaveatDefinition{}
			if err := caveat.UnmarshalVT(caveatDoc.Definition); err != nil {
				return fmt.Errorf("failed to unmarshal caveat definition: %w", err)
			}
			if err := tracked.AddChangedDefinition(ctx, revision, caveat); err != nil {
				return err
			}
		}
	}

	// Add deleted namespaces
	for _, nsName := range doc.Changes.DeletedNamespaces {
		if err := tracked.AddDeletedNamespace(ctx, revision, nsName); err != nil {
			return err
		}
	}

	// Add deleted caveats
	for _, caveatName := range doc.Changes.DeletedCaveats {
		if err := tracked.AddDeletedCaveat(ctx, revision, caveatName); err != nil {
			return err
		}
	}

	// Add metadata if present
	if len(doc.Changes.Metadata) > 0 {
		metadataMap := make(map[string]any)
		for k, v := range doc.Changes.Metadata {
			metadataMap[k] = v
		}
		if err := tracked.AddRevisionMetadata(ctx, revision, metadataMap); err != nil {
			return err
		}
	}

	return nil
}
