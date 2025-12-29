package mongodb

import (
	"time"
)

const (
	defaultGCInterval         = 3 * time.Minute
	defaultGCMaxOperationTime = 1 * time.Minute
)

// Option is a function that configures a MongoDB datastore.
type Option func(*mongoDBDatastore)

// GCWindow sets the garbage collection window for the datastore.
// Records older than this window are eligible for deletion.
func GCWindow(window time.Duration) Option {
	return func(m *mongoDBDatastore) {
		m.gcWindow = window
	}
}

// GCInterval sets the interval between garbage collection runs.
// Set to 0 to disable automatic garbage collection.
func GCInterval(interval time.Duration) Option {
	return func(m *mongoDBDatastore) {
		m.gcInterval = interval
	}
}

// GCMaxOperationTime sets the maximum time allowed for a single GC operation.
func GCMaxOperationTime(timeout time.Duration) Option {
	return func(m *mongoDBDatastore) {
		m.gcMaxOperationTime = timeout
	}
}

// GCEnabled enables or disables garbage collection.
func GCEnabled(enabled bool) Option {
	return func(m *mongoDBDatastore) {
		m.gcEnabled = enabled
	}
}

// RevisionQuantization sets the revision quantization interval.
func RevisionQuantization(interval time.Duration) Option {
	return func(m *mongoDBDatastore) {
		m.revisionQuantization = interval
	}
}

// WatchBufferLength sets the length of the watch buffer.
func WatchBufferLength(length uint16) Option {
	return func(m *mongoDBDatastore) {
		m.watchBufferLength = length
	}
}

// WatchBufferWriteTimeout sets the timeout for writing to the watch buffer.
func WatchBufferWriteTimeout(timeout time.Duration) Option {
	return func(m *mongoDBDatastore) {
		m.watchBufferWriteTimeout = timeout
	}
}

// MaxRevisionStalenessPercent sets the maximum staleness percentage for revisions.
func MaxRevisionStalenessPercent(percent float64) Option {
	return func(m *mongoDBDatastore) {
		m.maxRevisionStalenessPercent = percent
	}
}
