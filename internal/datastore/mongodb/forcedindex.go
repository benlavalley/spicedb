package mongodb

import (
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore/options"
)

// forcedIndex implements common.IndexingHint for MongoDB.
// It forces MongoDB to use a specific index for a query via the SetHint() option.
type forcedIndex struct {
	index common.IndexDefinition
}

// SQLPrefix returns an empty string as MongoDB doesn't use SQL prefixes.
func (f forcedIndex) SQLPrefix() (string, error) {
	return "", nil
}

// FromTable returns the table name unchanged as MongoDB doesn't use SQL table syntax.
func (f forcedIndex) FromTable(existingTableName string) (string, error) {
	return existingTableName, nil
}

// FromSQLSuffix returns an empty string as MongoDB doesn't use SQL suffixes.
func (f forcedIndex) FromSQLSuffix() (string, error) {
	return "", nil
}

// SortOrder returns the preferred sort order for this index.
func (f forcedIndex) SortOrder() options.SortOrder {
	return f.index.PreferredSortOrder
}

// IndexName returns the MongoDB index name to use with SetHint().
func (f forcedIndex) IndexName() string {
	return f.index.Name
}

var _ common.IndexingHint = forcedIndex{}
