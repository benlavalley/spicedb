package mongodb

import (
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// Collection names
const (
	colRelationships = "relationships"
	colNamespaces    = "namespaces"
	colCaveats       = "caveats"
	colCounters      = "counters"
	colChangelog     = "changelog"
	colMetadata      = "metadata"
)

// relationshipDoc represents a relationship document in MongoDB.
type relationshipDoc struct {
	Namespace        string     `bson:"namespace"`
	ResourceID       string     `bson:"resource_id"`
	Relation         string     `bson:"relation"`
	SubjectNamespace string     `bson:"subject_namespace"`
	SubjectObjectID  string     `bson:"subject_object_id"`
	SubjectRelation  string     `bson:"subject_relation"`
	CaveatName       string     `bson:"caveat_name,omitempty"`
	CaveatContext    bson.M     `bson:"caveat_context,omitempty"`
	IntegrityKeyID   string     `bson:"integrity_key_id,omitempty"`
	IntegrityHash    []byte     `bson:"integrity_hash,omitempty"`
	IntegrityTime    *time.Time `bson:"integrity_timestamp,omitempty"`
	Expiration       *time.Time `bson:"expiration,omitempty"`
	CreatedRevision  int64      `bson:"created_revision"`
	DeletedRevision  int64      `bson:"deleted_revision"`
}

// ToRelationship converts a relationshipDoc to a tuple.Relationship.
func (r *relationshipDoc) ToRelationship() (tuple.Relationship, error) {
	var caveat *core.ContextualizedCaveat
	if r.CaveatName != "" {
		contextMap := make(map[string]any)
		for k, v := range r.CaveatContext {
			contextMap[k] = v
		}
		contextStruct, err := structpb.NewStruct(contextMap)
		if err != nil {
			return tuple.Relationship{}, err
		}
		caveat = &core.ContextualizedCaveat{
			CaveatName: r.CaveatName,
			Context:    contextStruct,
		}
	}

	var integrity *core.RelationshipIntegrity
	if r.IntegrityKeyID != "" {
		integrity = &core.RelationshipIntegrity{
			KeyId: r.IntegrityKeyID,
			Hash:  r.IntegrityHash,
		}
		if r.IntegrityTime != nil {
			integrity.HashedAt = timestamppb.New(*r.IntegrityTime)
		}
	}

	return tuple.Relationship{
		RelationshipReference: tuple.RelationshipReference{
			Resource: tuple.ObjectAndRelation{
				ObjectType: r.Namespace,
				ObjectID:   r.ResourceID,
				Relation:   r.Relation,
			},
			Subject: tuple.ObjectAndRelation{
				ObjectType: r.SubjectNamespace,
				ObjectID:   r.SubjectObjectID,
				Relation:   r.SubjectRelation,
			},
		},
		OptionalCaveat:     caveat,
		OptionalIntegrity:  integrity,
		OptionalExpiration: r.Expiration,
	}, nil
}

// namespaceDoc represents a namespace document in MongoDB.
// Uses soft-delete pattern with created_revision/deleted_revision for point-in-time reads.
type namespaceDoc struct {
	Name            string `bson:"name"`
	Definition      []byte `bson:"definition"`
	CreatedRevision int64  `bson:"created_revision"`
	DeletedRevision int64  `bson:"deleted_revision"` // 0 means not deleted
}

// caveatDoc represents a caveat document in MongoDB.
// Uses soft-delete pattern with created_revision/deleted_revision for point-in-time reads.
type caveatDoc struct {
	Name            string `bson:"name"`
	Definition      []byte `bson:"definition"`
	CreatedRevision int64  `bson:"created_revision"`
	DeletedRevision int64  `bson:"deleted_revision"` // 0 means not deleted
}

// counterDoc represents a counter document in MongoDB.
type counterDoc struct {
	Name               string `bson:"_id"`
	Filter             []byte `bson:"filter"`
	Count              int    `bson:"count"`
	ComputedAtRevision int64  `bson:"computed_at_revision"`
}

// relationshipChangeDoc represents a relationship change in BSON format.
type relationshipChangeDoc struct {
	Operation        int32      `bson:"operation"` // tuple.UpdateOperation as int
	Namespace        string     `bson:"namespace"`
	ResourceID       string     `bson:"resource_id"`
	Relation         string     `bson:"relation"`
	SubjectNamespace string     `bson:"subject_namespace"`
	SubjectObjectID  string     `bson:"subject_object_id"`
	SubjectRelation  string     `bson:"subject_relation"`
	CaveatName       string     `bson:"caveat_name,omitempty"`
	CaveatContext    bson.M     `bson:"caveat_context,omitempty"`
	IntegrityKeyID   string     `bson:"integrity_key_id,omitempty"`
	IntegrityHash    []byte     `bson:"integrity_hash,omitempty"`
	IntegrityTime    *time.Time `bson:"integrity_timestamp,omitempty"`
	Expiration       *time.Time `bson:"expiration,omitempty"`
}

// namespaceChangeDoc represents a namespace change in the changelog.
type namespaceChangeDoc struct {
	Name       string `bson:"name"`
	Definition []byte `bson:"definition,omitempty"` // Serialized protobuf, empty if deleted
}

// caveatChangeDoc represents a caveat change in the changelog.
type caveatChangeDoc struct {
	Name       string `bson:"name"`
	Definition []byte `bson:"definition,omitempty"` // Serialized protobuf, empty if deleted
}

// changelogChangesDoc represents the changes in a changelog entry (BSON-friendly).
type changelogChangesDoc struct {
	RelationshipChanges []relationshipChangeDoc `bson:"relationship_changes,omitempty"`
	ChangedNamespaces   []namespaceChangeDoc    `bson:"changed_namespaces,omitempty"`
	ChangedCaveats      []caveatChangeDoc       `bson:"changed_caveats,omitempty"`
	DeletedNamespaces   []string                `bson:"deleted_namespaces,omitempty"`
	DeletedCaveats      []string                `bson:"deleted_caveats,omitempty"`
	IsCheckpoint        bool                    `bson:"is_checkpoint,omitempty"`
	Metadata            bson.M                  `bson:"metadata,omitempty"`
}

// changelogDoc represents a changelog entry in MongoDB.
type changelogDoc struct {
	Revision int64               `bson:"revision"`
	Changes  changelogChangesDoc `bson:"changes"`
}

// RelationshipUpdateToDoc converts a tuple.RelationshipUpdate to a BSON document.
func RelationshipUpdateToDoc(update tuple.RelationshipUpdate) relationshipChangeDoc {
	rel := update.Relationship
	doc := relationshipChangeDoc{
		Operation:        int32(update.Operation),
		Namespace:        rel.Resource.ObjectType,
		ResourceID:       rel.Resource.ObjectID,
		Relation:         rel.Resource.Relation,
		SubjectNamespace: rel.Subject.ObjectType,
		SubjectObjectID:  rel.Subject.ObjectID,
		SubjectRelation:  rel.Subject.Relation,
	}

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

	return doc
}

// ToRelationshipUpdate converts a relationshipChangeDoc back to tuple.RelationshipUpdate.
func (r *relationshipChangeDoc) ToRelationshipUpdate() (tuple.RelationshipUpdate, error) {
	var caveat *core.ContextualizedCaveat
	if r.CaveatName != "" {
		contextMap := make(map[string]any)
		for k, v := range r.CaveatContext {
			contextMap[k] = v
		}
		contextStruct, err := structpb.NewStruct(contextMap)
		if err != nil {
			return tuple.RelationshipUpdate{}, err
		}
		caveat = &core.ContextualizedCaveat{
			CaveatName: r.CaveatName,
			Context:    contextStruct,
		}
	}

	var integrity *core.RelationshipIntegrity
	if r.IntegrityKeyID != "" {
		integrity = &core.RelationshipIntegrity{
			KeyId: r.IntegrityKeyID,
			Hash:  r.IntegrityHash,
		}
		if r.IntegrityTime != nil {
			integrity.HashedAt = timestamppb.New(*r.IntegrityTime)
		}
	}

	return tuple.RelationshipUpdate{
		Operation: tuple.UpdateOperation(r.Operation),
		Relationship: tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ObjectAndRelation{
					ObjectType: r.Namespace,
					ObjectID:   r.ResourceID,
					Relation:   r.Relation,
				},
				Subject: tuple.ObjectAndRelation{
					ObjectType: r.SubjectNamespace,
					ObjectID:   r.SubjectObjectID,
					Relation:   r.SubjectRelation,
				},
			},
			OptionalCaveat:     caveat,
			OptionalIntegrity:  integrity,
			OptionalExpiration: r.Expiration,
		},
	}, nil
}

// metadataDoc represents datastore metadata in MongoDB.
type metadataDoc struct {
	Key   string `bson:"_id"`
	Value string `bson:"value"`
}

// uniqueIDKey is the key used to store the unique ID in metadata.
const uniqueIDKey = "unique_id"

// relationshipUniqueKey returns the unique key fields for a relationship.
func relationshipUniqueKey(r *relationshipDoc) bson.D {
	return bson.D{
		{Key: "namespace", Value: r.Namespace},
		{Key: "resource_id", Value: r.ResourceID},
		{Key: "relation", Value: r.Relation},
		{Key: "subject_namespace", Value: r.SubjectNamespace},
		{Key: "subject_object_id", Value: r.SubjectObjectID},
		{Key: "subject_relation", Value: r.SubjectRelation},
		{Key: "deleted_revision", Value: int64(0)},
	}
}
