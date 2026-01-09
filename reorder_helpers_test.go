package repository

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

type reorderTestRecord struct {
	ID   uuid.UUID
	Name string
}

func TestReorderRecordsByID_ReordersToInputOrder(t *testing.T) {
	id1 := uuid.New()
	id2 := uuid.New()

	order := []uuid.UUID{id1, id2}
	records := []reorderTestRecord{
		{ID: id2, Name: "second"},
		{ID: id1, Name: "first"},
	}

	reordered, ok := reorderRecordsByID(records, order, func(r reorderTestRecord) uuid.UUID {
		return r.ID
	})

	assert.True(t, ok)
	assert.Equal(t, id1, reordered[0].ID)
	assert.Equal(t, id2, reordered[1].ID)
}

func TestReorderRecordsByID_FailsOnNilID(t *testing.T) {
	order := []uuid.UUID{uuid.Nil}
	records := []reorderTestRecord{{ID: uuid.Nil}}

	reordered, ok := reorderRecordsByID(records, order, func(r reorderTestRecord) uuid.UUID {
		return r.ID
	})

	assert.False(t, ok)
	assert.Equal(t, records, reordered)
}

func TestReorderRecordsByID_FailsOnUnknownID(t *testing.T) {
	id1 := uuid.New()
	id2 := uuid.New()

	order := []uuid.UUID{id1}
	records := []reorderTestRecord{{ID: id2}}

	reordered, ok := reorderRecordsByID(records, order, func(r reorderTestRecord) uuid.UUID {
		return r.ID
	})

	assert.False(t, ok)
	assert.Equal(t, records, reordered)
}
