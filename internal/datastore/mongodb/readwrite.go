package mongodb

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	dsoptions "github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// mongoDBReadWriteTx implements the datastore.ReadWriteTransaction interface.
type mongoDBReadWriteTx struct {
	mongoDBReader
	session     *mongo.Session
	newRevision revisions.TimestampRevision
	config      *dsoptions.RWTOptions

	// Track changes for the changelog
	relChanges        []tuple.RelationshipUpdate
	changedNamespaces []*core.NamespaceDefinition
	changedCaveats    []*core.CaveatDefinition
	deletedNamespaces []string
	deletedCaveats    []string
}

// WriteRelationships writes relationship mutations.
func (rwt *mongoDBReadWriteTx) WriteRelationships(ctx context.Context, mutations []tuple.RelationshipUpdate) error {
	col := rwt.database.Collection(colRelationships)
	revisionNanos := rwt.newRevision.TimestampNanoSec()

	for _, mutation := range mutations {
		rel := mutation.Relationship

		// Build the document
		doc := relationshipDoc{
			Namespace:        rel.Resource.ObjectType,
			ResourceID:       rel.Resource.ObjectID,
			Relation:         rel.Resource.Relation,
			SubjectNamespace: rel.Subject.ObjectType,
			SubjectObjectID:  rel.Subject.ObjectID,
			SubjectRelation:  rel.Subject.Relation,
			CreatedRevision:  revisionNanos,
			DeletedRevision:  0,
		}

		// Add optional fields
		if rel.OptionalCaveat != nil {
			doc.CaveatName = rel.OptionalCaveat.CaveatName
			if rel.OptionalCaveat.Context != nil {
				doc.CaveatContext = bson.M{}
				for k, v := range rel.OptionalCaveat.Context.AsMap() {
					doc.CaveatContext[k] = v
				}
			}
		}

		if rel.OptionalIntegrity != nil {
			doc.IntegrityKeyID = rel.OptionalIntegrity.KeyId
			doc.IntegrityHash = rel.OptionalIntegrity.Hash
			if rel.OptionalIntegrity.HashedAt != nil {
				ts := rel.OptionalIntegrity.HashedAt.AsTime()
				doc.IntegrityTime = &ts
			}
		}

		if rel.OptionalExpiration != nil {
			doc.Expiration = rel.OptionalExpiration
		}

		// Find existing active relationship
		filter := relationshipUniqueKey(&doc)
		var existing relationshipDoc
		err := col.FindOne(ctx, filter).Decode(&existing)
		existsAlready := err == nil

		switch mutation.Operation {
		case tuple.UpdateOperationCreate:
			if existsAlready {
				existingRel, err := existing.ToRelationship()
				if err != nil {
					return err
				}
				return common.NewCreateRelationshipExistsError(&existingRel)
			}
			_, err := col.InsertOne(ctx, doc)
			if err != nil {
				if mongo.IsDuplicateKeyError(err) {
					existingRel, _ := existing.ToRelationship()
					return common.NewCreateRelationshipExistsError(&existingRel)
				}
				return fmt.Errorf("failed to insert relationship: %w", err)
			}
			// Track this change for the changelog
			rwt.relChanges = append(rwt.relChanges, mutation)

		case tuple.UpdateOperationTouch:
			if existsAlready {
				existingRel, err := existing.ToRelationship()
				if err != nil {
					return err
				}
				// Check if completely identical (including expiration/integrity)
				if relationshipsIdentical(existingRel, rel) {
					continue
				}
				// Soft delete the existing one
				_, err = col.UpdateOne(ctx, filter, bson.M{
					"$set": bson.M{"deleted_revision": revisionNanos},
				})
				if err != nil {
					return fmt.Errorf("failed to soft-delete existing relationship: %w", err)
				}
			}
			// Insert the new version
			_, err := col.InsertOne(ctx, doc)
			if err != nil {
				return fmt.Errorf("failed to insert relationship: %w", err)
			}
			// Track this change for the changelog
			rwt.relChanges = append(rwt.relChanges, mutation)

		case tuple.UpdateOperationDelete:
			if existsAlready {
				_, err := col.UpdateOne(ctx, filter, bson.M{
					"$set": bson.M{"deleted_revision": revisionNanos},
				})
				if err != nil {
					return fmt.Errorf("failed to delete relationship: %w", err)
				}
				// Only track if we actually deleted something
				rwt.relChanges = append(rwt.relChanges, mutation)
			}

		default:
			return spiceerrors.MustBugf("unknown tuple mutation operation type: %v", mutation.Operation)
		}
	}

	return nil
}

// DeleteRelationships deletes relationships matching the filter.
func (rwt *mongoDBReadWriteTx) DeleteRelationships(ctx context.Context, filter *v1.RelationshipFilter, opts ...dsoptions.DeleteOptionsOption) (uint64, bool, error) {
	delOpts := dsoptions.NewDeleteOptionsWithOptionsAndDefaults(opts...)

	col := rwt.database.Collection(colRelationships)
	revisionNanos := rwt.newRevision.TimestampNanoSec()

	// Build the MongoDB filter
	mongoFilter := bson.D{
		{Key: "deleted_revision", Value: int64(0)}, // Only active relationships
	}

	if filter.ResourceType != "" {
		mongoFilter = append(mongoFilter, bson.E{Key: "namespace", Value: filter.ResourceType})
	}
	if filter.OptionalResourceId != "" {
		mongoFilter = append(mongoFilter, bson.E{Key: "resource_id", Value: filter.OptionalResourceId})
	}
	if filter.OptionalResourceIdPrefix != "" {
		mongoFilter = append(mongoFilter, bson.E{Key: "resource_id", Value: bson.M{"$regex": "^" + filter.OptionalResourceIdPrefix}})
	}
	if filter.OptionalRelation != "" {
		mongoFilter = append(mongoFilter, bson.E{Key: "relation", Value: filter.OptionalRelation})
	}

	if subjectFilter := filter.OptionalSubjectFilter; subjectFilter != nil {
		mongoFilter = append(mongoFilter, bson.E{Key: "subject_namespace", Value: subjectFilter.SubjectType})
		if subjectFilter.OptionalSubjectId != "" {
			mongoFilter = append(mongoFilter, bson.E{Key: "subject_object_id", Value: subjectFilter.OptionalSubjectId})
		}
		if subjectFilter.OptionalRelation != nil {
			relation := subjectFilter.OptionalRelation.Relation
			if relation == "" {
				relation = datastore.Ellipsis
			}
			mongoFilter = append(mongoFilter, bson.E{Key: "subject_relation", Value: relation})
		}
	}

	var delLimit int64
	if delOpts.DeleteLimit != nil && *delOpts.DeleteLimit > 0 {
		delLimit = int64(*delOpts.DeleteLimit)
	}

	// Find relationships to delete first (so we can track them)
	findOpts := options.Find()
	if delLimit > 0 {
		findOpts = findOpts.SetLimit(delLimit)
	}

	cursor, err := col.Find(ctx, mongoFilter, findOpts)
	if err != nil {
		return 0, false, fmt.Errorf("failed to find relationships to delete: %w", err)
	}
	defer cursor.Close(ctx)

	var toDelete []relationshipDoc
	for cursor.Next(ctx) {
		var doc relationshipDoc
		if err := cursor.Decode(&doc); err != nil {
			return 0, false, fmt.Errorf("failed to decode relationship: %w", err)
		}
		toDelete = append(toDelete, doc)
	}
	if err := cursor.Err(); err != nil {
		return 0, false, err
	}

	if len(toDelete) == 0 {
		return 0, false, nil
	}

	// Update all matching relationships by their unique key combination
	result, err := col.UpdateMany(ctx,
		bson.M{
			"deleted_revision": int64(0),
			"$or": func() bson.A {
				arr := make(bson.A, len(toDelete))
				for i, doc := range toDelete {
					arr[i] = bson.M{
						"namespace":         doc.Namespace,
						"resource_id":       doc.ResourceID,
						"relation":          doc.Relation,
						"subject_namespace": doc.SubjectNamespace,
						"subject_object_id": doc.SubjectObjectID,
						"subject_relation":  doc.SubjectRelation,
						"deleted_revision":  int64(0),
					}
				}
				return arr
			}(),
		},
		bson.M{"$set": bson.M{"deleted_revision": revisionNanos}},
	)
	if err != nil {
		return 0, false, fmt.Errorf("failed to delete relationships: %w", err)
	}

	// Track deleted relationships for changelog
	for _, doc := range toDelete {
		rel, err := doc.ToRelationship()
		if err != nil {
			return uint64(result.ModifiedCount), false, err
		}
		rwt.relChanges = append(rwt.relChanges, tuple.RelationshipUpdate{
			Operation:    tuple.UpdateOperationDelete,
			Relationship: rel,
		})
	}

	// Check if there are more to delete (only relevant if limit was set)
	if delLimit > 0 {
		remaining, err := col.CountDocuments(ctx, mongoFilter)
		if err != nil {
			return uint64(result.ModifiedCount), false, err
		}
		return uint64(result.ModifiedCount), remaining > 0, nil
	}

	return uint64(result.ModifiedCount), false, nil
}

// WriteNamespaces writes namespace definitions using soft-delete pattern for versioning.
func (rwt *mongoDBReadWriteTx) WriteNamespaces(ctx context.Context, newConfigs ...*core.NamespaceDefinition) error {
	col := rwt.database.Collection(colNamespaces)
	revisionNanos := rwt.newRevision.TimestampNanoSec()

	for _, ns := range newConfigs {
		serialized, err := ns.MarshalVT()
		if err != nil {
			return err
		}

		// Soft-delete any existing active version of this namespace
		_, err = col.UpdateMany(ctx,
			bson.M{
				"name":             ns.Name,
				"deleted_revision": int64(0),
			},
			bson.M{
				"$set": bson.M{"deleted_revision": revisionNanos},
			},
		)
		if err != nil {
			return fmt.Errorf("failed to soft-delete existing namespace: %w", err)
		}

		// Insert the new version
		doc := namespaceDoc{
			Name:            ns.Name,
			Definition:      serialized,
			CreatedRevision: revisionNanos,
			DeletedRevision: 0,
		}

		_, err = col.InsertOne(ctx, doc)
		if err != nil {
			return fmt.Errorf("failed to write namespace: %w", err)
		}

		// Track the namespace change for the changelog
		rwt.changedNamespaces = append(rwt.changedNamespaces, ns)
	}

	return nil
}

// DeleteNamespaces deletes namespaces using soft-delete.
func (rwt *mongoDBReadWriteTx) DeleteNamespaces(ctx context.Context, nsNames []string, delOption datastore.DeleteNamespacesRelationshipsOption) error {
	if len(nsNames) == 0 {
		return nil
	}

	nsCol := rwt.database.Collection(colNamespaces)
	relCol := rwt.database.Collection(colRelationships)
	revisionNanos := rwt.newRevision.TimestampNanoSec()

	for _, nsName := range nsNames {
		// Check if namespace exists (active version)
		var doc namespaceDoc
		err := nsCol.FindOne(ctx, bson.M{"name": nsName, "deleted_revision": int64(0)}).Decode(&doc)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				return fmt.Errorf("namespace not found: %s", nsName)
			}
			return err
		}

		// Soft-delete the namespace
		_, err = nsCol.UpdateMany(ctx,
			bson.M{
				"name":             nsName,
				"deleted_revision": int64(0),
			},
			bson.M{
				"$set": bson.M{"deleted_revision": revisionNanos},
			},
		)
		if err != nil {
			return fmt.Errorf("failed to delete namespace: %w", err)
		}

		// Track the namespace deletion for the changelog
		rwt.deletedNamespaces = append(rwt.deletedNamespaces, nsName)

		// Optionally delete relationships
		if delOption == datastore.DeleteNamespacesAndRelationships {
			_, err = relCol.UpdateMany(ctx,
				bson.M{
					"namespace":        nsName,
					"deleted_revision": int64(0),
				},
				bson.M{
					"$set": bson.M{"deleted_revision": revisionNanos},
				},
			)
			if err != nil {
				return fmt.Errorf("failed to delete relationships: %w", err)
			}
		}
	}

	return nil
}

// WriteCaveats writes caveat definitions using soft-delete pattern for versioning.
func (rwt *mongoDBReadWriteTx) WriteCaveats(ctx context.Context, caveats []*core.CaveatDefinition) error {
	col := rwt.database.Collection(colCaveats)
	revisionNanos := rwt.newRevision.TimestampNanoSec()

	for _, caveat := range caveats {
		serialized, err := caveat.MarshalVT()
		if err != nil {
			return err
		}

		// Soft-delete any existing active version of this caveat
		_, err = col.UpdateMany(ctx,
			bson.M{
				"name":             caveat.Name,
				"deleted_revision": int64(0),
			},
			bson.M{
				"$set": bson.M{"deleted_revision": revisionNanos},
			},
		)
		if err != nil {
			return fmt.Errorf("failed to soft-delete existing caveat: %w", err)
		}

		// Insert the new version
		doc := caveatDoc{
			Name:            caveat.Name,
			Definition:      serialized,
			CreatedRevision: revisionNanos,
			DeletedRevision: 0,
		}

		_, err = col.InsertOne(ctx, doc)
		if err != nil {
			return fmt.Errorf("failed to write caveat: %w", err)
		}

		// Track the caveat change for the changelog
		rwt.changedCaveats = append(rwt.changedCaveats, caveat)
	}

	return nil
}

// DeleteCaveats deletes caveats using soft-delete.
func (rwt *mongoDBReadWriteTx) DeleteCaveats(ctx context.Context, names []string) error {
	if len(names) == 0 {
		return nil
	}

	col := rwt.database.Collection(colCaveats)
	revisionNanos := rwt.newRevision.TimestampNanoSec()

	// Soft-delete all matching caveats
	_, err := col.UpdateMany(ctx,
		bson.M{
			"name":             bson.M{"$in": names},
			"deleted_revision": int64(0),
		},
		bson.M{
			"$set": bson.M{"deleted_revision": revisionNanos},
		},
	)
	if err != nil {
		return err
	}

	// Track the caveat deletions for the changelog
	rwt.deletedCaveats = append(rwt.deletedCaveats, names...)
	return nil
}

// RegisterCounter registers a new counter.
func (rwt *mongoDBReadWriteTx) RegisterCounter(ctx context.Context, name string, filter *core.RelationshipFilter) error {
	col := rwt.database.Collection(colCounters)

	// Check if counter already exists
	var existing counterDoc
	err := col.FindOne(ctx, bson.M{"_id": name}).Decode(&existing)
	if err == nil {
		return datastore.NewCounterAlreadyRegisteredErr(name, filter)
	}
	if !errors.Is(err, mongo.ErrNoDocuments) {
		return err
	}

	filterBytes, err := filter.MarshalVT()
	if err != nil {
		return err
	}

	doc := counterDoc{
		Name:               name,
		Filter:             filterBytes,
		Count:              0,
		ComputedAtRevision: 0,
	}

	_, err = col.InsertOne(ctx, doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return datastore.NewCounterAlreadyRegisteredErr(name, filter)
		}
		return err
	}

	return nil
}

// UnregisterCounter unregisters a counter.
func (rwt *mongoDBReadWriteTx) UnregisterCounter(ctx context.Context, name string) error {
	col := rwt.database.Collection(colCounters)

	result, err := col.DeleteOne(ctx, bson.M{"_id": name})
	if err != nil {
		return err
	}
	if result.DeletedCount == 0 {
		return datastore.NewCounterNotRegisteredErr(name)
	}
	return nil
}

// StoreCounterValue stores a computed counter value.
func (rwt *mongoDBReadWriteTx) StoreCounterValue(ctx context.Context, name string, value int, computedAtRevision datastore.Revision) error {
	col := rwt.database.Collection(colCounters)

	var revNanos int64
	if computedAtRevision != nil && computedAtRevision != datastore.NoRevision {
		revNanos = computedAtRevision.(revisions.TimestampRevision).TimestampNanoSec()
	}

	result, err := col.UpdateOne(ctx,
		bson.M{"_id": name},
		bson.M{
			"$set": bson.M{
				"count":                value,
				"computed_at_revision": revNanos,
			},
		},
	)
	if err != nil {
		return err
	}
	if result.MatchedCount == 0 {
		return datastore.NewCounterNotRegisteredErr(name)
	}
	return nil
}

// BulkLoad loads relationships in bulk.
func (rwt *mongoDBReadWriteTx) BulkLoad(ctx context.Context, iter datastore.BulkWriteRelationshipSource) (uint64, error) {
	col := rwt.database.Collection(colRelationships)
	revisionNanos := rwt.newRevision.TimestampNanoSec()

	var numCopied uint64
	var docs []any
	batchSize := 1000

	for {
		next, err := iter.Next(ctx)
		if err != nil {
			return numCopied, err
		}
		if next == nil {
			break
		}

		doc := relationshipDoc{
			Namespace:        next.Resource.ObjectType,
			ResourceID:       next.Resource.ObjectID,
			Relation:         next.Resource.Relation,
			SubjectNamespace: next.Subject.ObjectType,
			SubjectObjectID:  next.Subject.ObjectID,
			SubjectRelation:  next.Subject.Relation,
			CreatedRevision:  revisionNanos,
			DeletedRevision:  0,
		}

		if next.OptionalCaveat != nil {
			doc.CaveatName = next.OptionalCaveat.CaveatName
			if next.OptionalCaveat.Context != nil {
				doc.CaveatContext = bson.M{}
				for k, v := range next.OptionalCaveat.Context.AsMap() {
					doc.CaveatContext[k] = v
				}
			}
		}

		if next.OptionalIntegrity != nil {
			doc.IntegrityKeyID = next.OptionalIntegrity.KeyId
			doc.IntegrityHash = next.OptionalIntegrity.Hash
			if next.OptionalIntegrity.HashedAt != nil {
				ts := next.OptionalIntegrity.HashedAt.AsTime()
				doc.IntegrityTime = &ts
			}
		}

		if next.OptionalExpiration != nil {
			doc.Expiration = next.OptionalExpiration
		}

		docs = append(docs, doc)

		if len(docs) >= batchSize {
			result, err := col.InsertMany(ctx, docs)
			if err != nil {
				if mongo.IsDuplicateKeyError(err) {
					return numCopied, common.NewCreateRelationshipExistsError(nil)
				}
				return numCopied, fmt.Errorf("failed to bulk insert: %w", err)
			}
			numCopied += uint64(len(result.InsertedIDs))
			docs = docs[:0]
		}
	}

	// Insert remaining docs
	if len(docs) > 0 {
		result, err := col.InsertMany(ctx, docs)
		if err != nil {
			if mongo.IsDuplicateKeyError(err) {
				return numCopied, common.NewCreateRelationshipExistsError(nil)
			}
			return numCopied, fmt.Errorf("failed to bulk insert: %w", err)
		}
		numCopied += uint64(len(result.InsertedIDs))
	}

	return numCopied, nil
}

// writeChangelog writes the accumulated changes to the changelog collection.
func (rwt *mongoDBReadWriteTx) writeChangelog(ctx context.Context) error {
	// Check if there are any changes to write
	hasChanges := len(rwt.relChanges) > 0 ||
		len(rwt.changedNamespaces) > 0 ||
		len(rwt.changedCaveats) > 0 ||
		len(rwt.deletedNamespaces) > 0 ||
		len(rwt.deletedCaveats) > 0

	if !hasChanges {
		return nil
	}

	col := rwt.database.Collection(colChangelog)
	revisionNanos := rwt.newRevision.TimestampNanoSec()

	// Convert relationship updates to BSON-friendly format
	relChangeDocs := make([]relationshipChangeDoc, len(rwt.relChanges))
	for i, change := range rwt.relChanges {
		relChangeDocs[i] = RelationshipUpdateToDoc(change)
	}

	// Convert namespace changes to BSON-friendly format
	nsChangeDocs := make([]namespaceChangeDoc, len(rwt.changedNamespaces))
	for i, ns := range rwt.changedNamespaces {
		serialized, err := ns.MarshalVT()
		if err != nil {
			return fmt.Errorf("failed to serialize namespace for changelog: %w", err)
		}
		nsChangeDocs[i] = namespaceChangeDoc{
			Name:       ns.Name,
			Definition: serialized,
		}
	}

	// Convert caveat changes to BSON-friendly format
	caveatChangeDocs := make([]caveatChangeDoc, len(rwt.changedCaveats))
	for i, caveat := range rwt.changedCaveats {
		serialized, err := caveat.MarshalVT()
		if err != nil {
			return fmt.Errorf("failed to serialize caveat for changelog: %w", err)
		}
		caveatChangeDocs[i] = caveatChangeDoc{
			Name:       caveat.Name,
			Definition: serialized,
		}
	}

	changes := changelogChangesDoc{
		RelationshipChanges: relChangeDocs,
		ChangedNamespaces:   nsChangeDocs,
		ChangedCaveats:      caveatChangeDocs,
		DeletedNamespaces:   rwt.deletedNamespaces,
		DeletedCaveats:      rwt.deletedCaveats,
	}

	// Include transaction metadata if present
	if rwt.config != nil && rwt.config.Metadata != nil {
		changes.Metadata = bson.M{}
		for k, v := range rwt.config.Metadata.AsMap() {
			changes.Metadata[k] = v
		}
	}

	doc := changelogDoc{
		Revision: revisionNanos,
		Changes:  changes,
	}

	_, err := col.InsertOne(ctx, doc)
	if err != nil {
		return fmt.Errorf("failed to write changelog: %w", err)
	}

	return nil
}

// relationshipsIdentical checks if two relationships are completely identical,
// including optional fields like expiration and integrity.
func relationshipsIdentical(a, b tuple.Relationship) bool {
	// Check core fields
	if tuple.MustString(a) != tuple.MustString(b) {
		return false
	}

	// Check expiration
	if (a.OptionalExpiration == nil) != (b.OptionalExpiration == nil) {
		return false
	}
	if a.OptionalExpiration != nil && b.OptionalExpiration != nil {
		if !a.OptionalExpiration.Equal(*b.OptionalExpiration) {
			return false
		}
	}

	// Check integrity
	if (a.OptionalIntegrity == nil) != (b.OptionalIntegrity == nil) {
		return false
	}
	if a.OptionalIntegrity != nil && b.OptionalIntegrity != nil {
		if a.OptionalIntegrity.KeyId != b.OptionalIntegrity.KeyId {
			return false
		}
		if string(a.OptionalIntegrity.Hash) != string(b.OptionalIntegrity.Hash) {
			return false
		}
	}

	return true
}

var _ datastore.ReadWriteTransaction = &mongoDBReadWriteTx{}
