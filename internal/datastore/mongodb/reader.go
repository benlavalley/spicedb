package mongodb

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mongooptions "go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// mongoDBReader implements the datastore.Reader interface.
type mongoDBReader struct {
	database *mongo.Database
	revision revisions.TimestampRevision
	initErr  error
	now      time.Time
}

// QueryRelationships reads relationships starting from the resource side.
func (r *mongoDBReader) QueryRelationships(
	ctx context.Context,
	filter datastore.RelationshipsFilter,
	opts ...options.QueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	if r.initErr != nil {
		return nil, r.initErr
	}

	queryOpts := options.NewQueryOptionsWithOptions(opts...)

	// Build the query filter
	mongoFilter := r.buildRelationshipFilter(filter)

	// Add revision filter - only include relationships that existed at this revision
	revisionNanos := r.revision.TimestampNanoSec()
	mongoFilter = append(mongoFilter,
		bson.E{Key: "created_revision", Value: bson.M{"$lte": revisionNanos}},
		bson.E{Key: "$or", Value: bson.A{
			bson.M{"deleted_revision": int64(0)},
			bson.M{"deleted_revision": bson.M{"$gt": revisionNanos}},
		}},
	)

	// Build find options
	findOpts := mongooptions.Find()
	if queryOpts.Limit != nil {
		findOpts.SetLimit(int64(*queryOpts.Limit))
	}

	// Apply sorting
	switch queryOpts.Sort {
	case options.ByResource:
		findOpts.SetSort(bson.D{
			{Key: "namespace", Value: 1},
			{Key: "resource_id", Value: 1},
			{Key: "relation", Value: 1},
			{Key: "subject_namespace", Value: 1},
			{Key: "subject_object_id", Value: 1},
			{Key: "subject_relation", Value: 1},
		})
	case options.BySubject:
		findOpts.SetSort(bson.D{
			{Key: "subject_namespace", Value: 1},
			{Key: "subject_object_id", Value: 1},
			{Key: "subject_relation", Value: 1},
			{Key: "namespace", Value: 1},
			{Key: "resource_id", Value: 1},
			{Key: "relation", Value: 1},
		})
	}

	// Apply index hint based on query shape
	if indexHint := IndexingHintForQueryShape(queryOpts.QueryShape); indexHint != nil {
		if hint, ok := indexHint.(forcedIndex); ok {
			findOpts.SetHint(hint.IndexName())
		}
	}

	// Handle cursor (after)
	if queryOpts.After != nil {
		cursorFilter := r.buildCursorFilter(queryOpts.After, queryOpts.Sort)
		mongoFilter = append(mongoFilter, cursorFilter...)
	}

	col := r.database.Collection(colRelationships)
	cursor, err := col.Find(ctx, mongoFilter, findOpts)
	if err != nil {
		return nil, err
	}

	return r.cursorToIterator(ctx, cursor, queryOpts.SkipCaveats, queryOpts.SkipExpiration), nil
}

// ReverseQueryRelationships reads relationships starting from the subject.
func (r *mongoDBReader) ReverseQueryRelationships(
	ctx context.Context,
	subjectsFilter datastore.SubjectsFilter,
	opts ...options.ReverseQueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	if r.initErr != nil {
		return nil, r.initErr
	}

	queryOpts := options.NewReverseQueryOptionsWithOptions(opts...)

	// Build the query filter
	mongoFilter := bson.D{
		{Key: "subject_namespace", Value: subjectsFilter.SubjectType},
	}

	if len(subjectsFilter.OptionalSubjectIds) > 0 {
		mongoFilter = append(mongoFilter, bson.E{
			Key:   "subject_object_id",
			Value: bson.M{"$in": subjectsFilter.OptionalSubjectIds},
		})
	}

	// Handle relation filter
	if !subjectsFilter.RelationFilter.IsEmpty() {
		var relations []string
		if subjectsFilter.RelationFilter.IncludeEllipsisRelation {
			relations = append(relations, datastore.Ellipsis)
		}
		if subjectsFilter.RelationFilter.NonEllipsisRelation != "" {
			relations = append(relations, subjectsFilter.RelationFilter.NonEllipsisRelation)
		}
		if subjectsFilter.RelationFilter.OnlyNonEllipsisRelations {
			mongoFilter = append(mongoFilter, bson.E{
				Key:   "subject_relation",
				Value: bson.M{"$ne": datastore.Ellipsis},
			})
		} else if len(relations) > 0 {
			mongoFilter = append(mongoFilter, bson.E{
				Key:   "subject_relation",
				Value: bson.M{"$in": relations},
			})
		}
	}

	// Add resource filter if specified
	if queryOpts.ResRelation != nil {
		if queryOpts.ResRelation.Namespace != "" {
			mongoFilter = append(mongoFilter, bson.E{
				Key:   "namespace",
				Value: queryOpts.ResRelation.Namespace,
			})
		}
		if queryOpts.ResRelation.Relation != "" {
			mongoFilter = append(mongoFilter, bson.E{
				Key:   "relation",
				Value: queryOpts.ResRelation.Relation,
			})
		}
	}

	// Add revision filter
	revisionNanos := r.revision.TimestampNanoSec()
	mongoFilter = append(mongoFilter,
		bson.E{Key: "created_revision", Value: bson.M{"$lte": revisionNanos}},
		bson.E{Key: "$or", Value: bson.A{
			bson.M{"deleted_revision": int64(0)},
			bson.M{"deleted_revision": bson.M{"$gt": revisionNanos}},
		}},
	)

	findOpts := mongooptions.Find()
	if queryOpts.LimitForReverse != nil {
		findOpts.SetLimit(int64(*queryOpts.LimitForReverse))
	}

	// Apply index hint based on query shape for reverse queries
	if indexHint := IndexingHintForQueryShape(queryOpts.QueryShapeForReverse); indexHint != nil {
		if hint, ok := indexHint.(forcedIndex); ok {
			findOpts.SetHint(hint.IndexName())
		}
	}

	col := r.database.Collection(colRelationships)
	cursor, err := col.Find(ctx, mongoFilter, findOpts)
	if err != nil {
		return nil, err
	}

	return r.cursorToIterator(ctx, cursor, queryOpts.SkipCaveatsForReverse, queryOpts.SkipExpirationForReverse), nil
}

// ReadNamespaceByName reads a namespace definition by name at the current revision.
func (r *mongoDBReader) ReadNamespaceByName(ctx context.Context, nsName string) (*core.NamespaceDefinition, datastore.Revision, error) {
	if r.initErr != nil {
		return nil, datastore.NoRevision, r.initErr
	}

	col := r.database.Collection(colNamespaces)
	revisionNanos := r.revision.TimestampNanoSec()

	// Filter for the namespace at the specified revision using point-in-time semantics
	filter := bson.D{
		{Key: "name", Value: nsName},
		{Key: "created_revision", Value: bson.M{"$lte": revisionNanos}},
		{Key: "$or", Value: bson.A{
			bson.M{"deleted_revision": int64(0)},
			bson.M{"deleted_revision": bson.M{"$gt": revisionNanos}},
		}},
	}

	var doc namespaceDoc
	err := col.FindOne(ctx, filter).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, datastore.NoRevision, datastore.NewNamespaceNotFoundErr(nsName)
		}
		return nil, datastore.NoRevision, err
	}

	ns := &core.NamespaceDefinition{}
	if err := ns.UnmarshalVT(doc.Definition); err != nil {
		return nil, datastore.NoRevision, err
	}

	return ns, revisions.NewForTimestamp(doc.CreatedRevision), nil
}

// ListAllNamespaces lists all namespaces at the current revision.
func (r *mongoDBReader) ListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	if r.initErr != nil {
		return nil, r.initErr
	}

	col := r.database.Collection(colNamespaces)
	revisionNanos := r.revision.TimestampNanoSec()

	// Filter for namespaces that existed at the specified revision
	filter := bson.D{
		{Key: "created_revision", Value: bson.M{"$lte": revisionNanos}},
		{Key: "$or", Value: bson.A{
			bson.M{"deleted_revision": int64(0)},
			bson.M{"deleted_revision": bson.M{"$gt": revisionNanos}},
		}},
	}

	cursor, err := col.Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var namespaces []datastore.RevisionedNamespace
	for cursor.Next(ctx) {
		var doc namespaceDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}

		ns := &core.NamespaceDefinition{}
		if err := ns.UnmarshalVT(doc.Definition); err != nil {
			return nil, err
		}

		namespaces = append(namespaces, datastore.RevisionedNamespace{
			Definition:          ns,
			LastWrittenRevision: revisions.NewForTimestamp(doc.CreatedRevision),
		})
	}

	return namespaces, cursor.Err()
}

// LookupNamespacesWithNames finds namespaces with the matching names at the current revision.
func (r *mongoDBReader) LookupNamespacesWithNames(ctx context.Context, nsNames []string) ([]datastore.RevisionedNamespace, error) {
	if r.initErr != nil {
		return nil, r.initErr
	}

	if len(nsNames) == 0 {
		return nil, nil
	}

	col := r.database.Collection(colNamespaces)
	revisionNanos := r.revision.TimestampNanoSec()

	// Filter for namespaces that existed at the specified revision
	filter := bson.D{
		{Key: "name", Value: bson.M{"$in": nsNames}},
		{Key: "created_revision", Value: bson.M{"$lte": revisionNanos}},
		{Key: "$or", Value: bson.A{
			bson.M{"deleted_revision": int64(0)},
			bson.M{"deleted_revision": bson.M{"$gt": revisionNanos}},
		}},
	}

	cursor, err := col.Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var namespaces []datastore.RevisionedNamespace
	for cursor.Next(ctx) {
		var doc namespaceDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}

		ns := &core.NamespaceDefinition{}
		if err := ns.UnmarshalVT(doc.Definition); err != nil {
			return nil, err
		}

		namespaces = append(namespaces, datastore.RevisionedNamespace{
			Definition:          ns,
			LastWrittenRevision: revisions.NewForTimestamp(doc.CreatedRevision),
		})
	}

	return namespaces, cursor.Err()
}

// buildRelationshipFilter builds a MongoDB filter from a RelationshipsFilter.
func (r *mongoDBReader) buildRelationshipFilter(filter datastore.RelationshipsFilter) bson.D {
	mongoFilter := bson.D{}

	if filter.OptionalResourceType != "" {
		mongoFilter = append(mongoFilter, bson.E{Key: "namespace", Value: filter.OptionalResourceType})
	}

	if len(filter.OptionalResourceIds) > 0 {
		mongoFilter = append(mongoFilter, bson.E{Key: "resource_id", Value: bson.M{"$in": filter.OptionalResourceIds}})
	}

	if filter.OptionalResourceIDPrefix != "" {
		mongoFilter = append(mongoFilter, bson.E{
			Key:   "resource_id",
			Value: bson.M{"$regex": "^" + filter.OptionalResourceIDPrefix},
		})
	}

	if filter.OptionalResourceRelation != "" {
		mongoFilter = append(mongoFilter, bson.E{Key: "relation", Value: filter.OptionalResourceRelation})
	}

	// Handle subject selectors
	if len(filter.OptionalSubjectsSelectors) > 0 {
		var orConditions []bson.M
		for _, selector := range filter.OptionalSubjectsSelectors {
			condition := bson.M{}

			if selector.OptionalSubjectType != "" {
				condition["subject_namespace"] = selector.OptionalSubjectType
			}

			if len(selector.OptionalSubjectIds) > 0 {
				condition["subject_object_id"] = bson.M{"$in": selector.OptionalSubjectIds}
			}

			if !selector.RelationFilter.IsEmpty() {
				var relations []string
				if selector.RelationFilter.IncludeEllipsisRelation {
					relations = append(relations, datastore.Ellipsis)
				}
				if selector.RelationFilter.NonEllipsisRelation != "" {
					relations = append(relations, selector.RelationFilter.NonEllipsisRelation)
				}
				if selector.RelationFilter.OnlyNonEllipsisRelations {
					condition["subject_relation"] = bson.M{"$ne": datastore.Ellipsis}
				} else if len(relations) > 0 {
					condition["subject_relation"] = bson.M{"$in": relations}
				}
			}

			if len(condition) > 0 {
				orConditions = append(orConditions, condition)
			}
		}
		if len(orConditions) > 0 {
			mongoFilter = append(mongoFilter, bson.E{Key: "$or", Value: orConditions})
		}
	}

	// Handle caveat filter
	switch filter.OptionalCaveatNameFilter.Option {
	case datastore.CaveatFilterOptionHasMatchingCaveat:
		mongoFilter = append(mongoFilter, bson.E{Key: "caveat_name", Value: filter.OptionalCaveatNameFilter.CaveatName})
	case datastore.CaveatFilterOptionNoCaveat:
		mongoFilter = append(mongoFilter, bson.E{Key: "$or", Value: bson.A{
			bson.M{"caveat_name": bson.M{"$exists": false}},
			bson.M{"caveat_name": ""},
		}})
	}

	// Handle expiration filter
	switch filter.OptionalExpirationOption {
	case datastore.ExpirationFilterOptionHasExpiration:
		mongoFilter = append(mongoFilter, bson.E{Key: "expiration", Value: bson.M{"$ne": nil}})
	case datastore.ExpirationFilterOptionNoExpiration:
		mongoFilter = append(mongoFilter, bson.E{Key: "expiration", Value: nil})
	}

	return mongoFilter
}

// buildCursorFilter builds a cursor filter for pagination.
func (r *mongoDBReader) buildCursorFilter(cursor options.Cursor, sort options.SortOrder) bson.D {
	// For cursor-based pagination, we need to find records after the cursor position
	switch sort {
	case options.ByResource:
		return bson.D{{Key: "$or", Value: bson.A{
			bson.M{"namespace": bson.M{"$gt": cursor.Resource.ObjectType}},
			bson.M{
				"namespace":   cursor.Resource.ObjectType,
				"resource_id": bson.M{"$gt": cursor.Resource.ObjectID},
			},
			bson.M{
				"namespace":   cursor.Resource.ObjectType,
				"resource_id": cursor.Resource.ObjectID,
				"relation":    bson.M{"$gt": cursor.Resource.Relation},
			},
			bson.M{
				"namespace":         cursor.Resource.ObjectType,
				"resource_id":       cursor.Resource.ObjectID,
				"relation":          cursor.Resource.Relation,
				"subject_namespace": bson.M{"$gt": cursor.Subject.ObjectType},
			},
			bson.M{
				"namespace":         cursor.Resource.ObjectType,
				"resource_id":       cursor.Resource.ObjectID,
				"relation":          cursor.Resource.Relation,
				"subject_namespace": cursor.Subject.ObjectType,
				"subject_object_id": bson.M{"$gt": cursor.Subject.ObjectID},
			},
			bson.M{
				"namespace":         cursor.Resource.ObjectType,
				"resource_id":       cursor.Resource.ObjectID,
				"relation":          cursor.Resource.Relation,
				"subject_namespace": cursor.Subject.ObjectType,
				"subject_object_id": cursor.Subject.ObjectID,
				"subject_relation":  bson.M{"$gt": cursor.Subject.Relation},
			},
		}}}
	case options.BySubject:
		return bson.D{{Key: "$or", Value: bson.A{
			bson.M{"subject_namespace": bson.M{"$gt": cursor.Subject.ObjectType}},
			bson.M{
				"subject_namespace": cursor.Subject.ObjectType,
				"subject_object_id": bson.M{"$gt": cursor.Subject.ObjectID},
			},
			bson.M{
				"subject_namespace": cursor.Subject.ObjectType,
				"subject_object_id": cursor.Subject.ObjectID,
				"subject_relation":  bson.M{"$gt": cursor.Subject.Relation},
			},
			bson.M{
				"subject_namespace": cursor.Subject.ObjectType,
				"subject_object_id": cursor.Subject.ObjectID,
				"subject_relation":  cursor.Subject.Relation,
				"namespace":         bson.M{"$gt": cursor.Resource.ObjectType},
			},
			bson.M{
				"subject_namespace": cursor.Subject.ObjectType,
				"subject_object_id": cursor.Subject.ObjectID,
				"subject_relation":  cursor.Subject.Relation,
				"namespace":         cursor.Resource.ObjectType,
				"resource_id":       bson.M{"$gt": cursor.Resource.ObjectID},
			},
			bson.M{
				"subject_namespace": cursor.Subject.ObjectType,
				"subject_object_id": cursor.Subject.ObjectID,
				"subject_relation":  cursor.Subject.Relation,
				"namespace":         cursor.Resource.ObjectType,
				"resource_id":       cursor.Resource.ObjectID,
				"relation":          bson.M{"$gt": cursor.Resource.Relation},
			},
		}}}
	default:
		return nil
	}
}

// cursorToIterator converts a MongoDB cursor to a RelationshipIterator.
func (r *mongoDBReader) cursorToIterator(ctx context.Context, cursor *mongo.Cursor, skipCaveats bool, skipExpiration bool) datastore.RelationshipIterator {
	return func(yield func(tuple.Relationship, error) bool) {
		defer cursor.Close(ctx)

		for cursor.Next(ctx) {
			var doc relationshipDoc
			if err := cursor.Decode(&doc); err != nil {
				if !yield(tuple.Relationship{}, err) {
					return
				}
				continue
			}

			// Skip expired relationships
			if doc.Expiration != nil && doc.Expiration.Before(r.now) {
				continue
			}

			rel, err := doc.ToRelationship()
			if err != nil {
				if !yield(tuple.Relationship{}, err) {
					return
				}
				continue
			}

			// Apply skip filters
			if skipCaveats && rel.OptionalCaveat != nil {
				continue
			}
			if skipExpiration && rel.OptionalExpiration != nil {
				continue
			}

			if !yield(rel, nil) {
				return
			}
		}

		if err := cursor.Err(); err != nil {
			yield(tuple.Relationship{}, err)
		}
	}
}

// CountRelationships counts relationships matching a registered counter.
func (r *mongoDBReader) CountRelationships(ctx context.Context, name string) (int, error) {
	if r.initErr != nil {
		return 0, r.initErr
	}

	counters, err := r.LookupCounters(ctx)
	if err != nil {
		return 0, err
	}

	var found *core.RelationshipFilter
	for _, counter := range counters {
		if counter.Name == name {
			found = counter.Filter
			break
		}
	}

	if found == nil {
		return 0, datastore.NewCounterNotRegisteredErr(name)
	}

	coreFilter, err := datastore.RelationshipsFilterFromCoreFilter(found)
	if err != nil {
		return 0, err
	}

	iter, err := r.QueryRelationships(ctx, coreFilter)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, err := range iter {
		if err != nil {
			return 0, err
		}
		count++
	}
	return count, nil
}

// LookupCounters returns all registered counters.
func (r *mongoDBReader) LookupCounters(ctx context.Context) ([]datastore.RelationshipCounter, error) {
	if r.initErr != nil {
		return nil, r.initErr
	}

	col := r.database.Collection(colCounters)
	cursor, err := col.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var counters []datastore.RelationshipCounter
	for cursor.Next(ctx) {
		var doc counterDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}

		filter := &core.RelationshipFilter{}
		if err := filter.UnmarshalVT(doc.Filter); err != nil {
			return nil, err
		}

		var computedAtRevision datastore.Revision
		if doc.ComputedAtRevision > 0 {
			computedAtRevision = revisions.NewForTimestamp(doc.ComputedAtRevision)
		} else {
			computedAtRevision = datastore.NoRevision
		}

		counters = append(counters, datastore.RelationshipCounter{
			Name:               doc.Name,
			Filter:             filter,
			Count:              doc.Count,
			ComputedAtRevision: computedAtRevision,
		})
	}

	return counters, cursor.Err()
}

// ReadCaveatByName reads a caveat by name at the current revision.
func (r *mongoDBReader) ReadCaveatByName(ctx context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	if r.initErr != nil {
		return nil, datastore.NoRevision, r.initErr
	}

	col := r.database.Collection(colCaveats)
	revisionNanos := r.revision.TimestampNanoSec()

	// Filter for the caveat at the specified revision using point-in-time semantics
	filter := bson.D{
		{Key: "name", Value: name},
		{Key: "created_revision", Value: bson.M{"$lte": revisionNanos}},
		{Key: "$or", Value: bson.A{
			bson.M{"deleted_revision": int64(0)},
			bson.M{"deleted_revision": bson.M{"$gt": revisionNanos}},
		}},
	}

	var doc caveatDoc
	err := col.FindOne(ctx, filter).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, datastore.NoRevision, datastore.NewCaveatNameNotFoundErr(name)
		}
		return nil, datastore.NoRevision, err
	}

	caveat := &core.CaveatDefinition{}
	if err := caveat.UnmarshalVT(doc.Definition); err != nil {
		return nil, datastore.NoRevision, err
	}

	return caveat, revisions.NewForTimestamp(doc.CreatedRevision), nil
}

// ListAllCaveats lists all caveats at the current revision.
func (r *mongoDBReader) ListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	if r.initErr != nil {
		return nil, r.initErr
	}

	col := r.database.Collection(colCaveats)
	revisionNanos := r.revision.TimestampNanoSec()

	// Filter for caveats that existed at the specified revision
	filter := bson.D{
		{Key: "created_revision", Value: bson.M{"$lte": revisionNanos}},
		{Key: "$or", Value: bson.A{
			bson.M{"deleted_revision": int64(0)},
			bson.M{"deleted_revision": bson.M{"$gt": revisionNanos}},
		}},
	}

	cursor, err := col.Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var caveats []datastore.RevisionedCaveat
	for cursor.Next(ctx) {
		var doc caveatDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}

		caveat := &core.CaveatDefinition{}
		if err := caveat.UnmarshalVT(doc.Definition); err != nil {
			return nil, err
		}

		caveats = append(caveats, datastore.RevisionedCaveat{
			Definition:          caveat,
			LastWrittenRevision: revisions.NewForTimestamp(doc.CreatedRevision),
		})
	}

	return caveats, cursor.Err()
}

// LookupCaveatsWithNames finds caveats with the matching names at the current revision.
func (r *mongoDBReader) LookupCaveatsWithNames(ctx context.Context, caveatNames []string) ([]datastore.RevisionedCaveat, error) {
	if r.initErr != nil {
		return nil, r.initErr
	}

	if len(caveatNames) == 0 {
		return nil, nil
	}

	col := r.database.Collection(colCaveats)
	revisionNanos := r.revision.TimestampNanoSec()

	// Filter for caveats that existed at the specified revision
	filter := bson.D{
		{Key: "name", Value: bson.M{"$in": caveatNames}},
		{Key: "created_revision", Value: bson.M{"$lte": revisionNanos}},
		{Key: "$or", Value: bson.A{
			bson.M{"deleted_revision": int64(0)},
			bson.M{"deleted_revision": bson.M{"$gt": revisionNanos}},
		}},
	}

	cursor, err := col.Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var caveats []datastore.RevisionedCaveat
	for cursor.Next(ctx) {
		var doc caveatDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}

		caveat := &core.CaveatDefinition{}
		if err := caveat.UnmarshalVT(doc.Definition); err != nil {
			return nil, err
		}

		caveats = append(caveats, datastore.RevisionedCaveat{
			Definition:          caveat,
			LastWrittenRevision: revisions.NewForTimestamp(doc.CreatedRevision),
		})
	}

	return caveats, cursor.Err()
}

// Unused but need to satisfy interface - keeping for reference
var (
	_ = slices.Contains[[]string]
	_ = strings.HasPrefix
	_ = common.NewSliceRelationshipIterator
)

var _ datastore.Reader = &mongoDBReader{}
