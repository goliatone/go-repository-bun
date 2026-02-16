package repository

import (
	"context"
	stderrors "errors"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mapHelperModel struct {
	ID        uuid.UUID  `bun:"id,pk,notnull" json:"id"`
	Name      string     `bun:"name,notnull" json:"name"`
	Count     int        `bun:"count,notnull" json:"count,omitempty"`
	Enabled   *bool      `bun:"enabled" json:"enabled,omitempty"`
	Hidden    string     `bun:"hidden" json:"-"`
	NoPersist string     `bun:"-" json:"no_persist"`
	CreatedAt *time.Time `bun:"created_at" json:"created_at,omitempty"`
}

type mapHelperUnsignedModel struct {
	Value uint64 `bun:"value" json:"value"`
}

func TestRecordToMap_DefaultUsesBunKeys(t *testing.T) {
	ts := time.Now().UTC().Truncate(time.Second)
	model := mapHelperModel{
		ID:        uuid.New(),
		Name:      "Alice",
		Count:     0,
		Enabled:   nil,
		Hidden:    "visible-in-bun-mode",
		NoPersist: "ignored",
		CreatedAt: &ts,
	}

	projected, err := RecordToMap(model)
	require.NoError(t, err)

	assert.Equal(t, model.ID, projected["id"])
	assert.Equal(t, "Alice", projected["name"])
	assert.Equal(t, 0, projected["count"])
	assert.Equal(t, "visible-in-bun-mode", projected["hidden"])
	assert.Equal(t, ts, projected["created_at"])
	assert.Contains(t, projected, "enabled")
	assert.Nil(t, projected["enabled"])
	assert.NotContains(t, projected, "NoPersist")
	assert.NotContains(t, projected, "no_persist")
}

func TestRecordToMap_JSONModeUsesJSONKeys(t *testing.T) {
	model := mapHelperModel{
		ID:     uuid.New(),
		Name:   "Alice",
		Hidden: "hidden",
	}

	projected, err := RecordToMap(model, WithProjectionKeyMode(MapKeyJSON))
	require.NoError(t, err)

	assert.Equal(t, model.ID, projected["id"])
	assert.Equal(t, "Alice", projected["name"])
	assert.NotContains(t, projected, "hidden")
}

func TestRecordToMap_CanOmitNilPointers(t *testing.T) {
	model := mapHelperModel{
		ID:      uuid.New(),
		Name:    "Alice",
		Enabled: nil,
	}

	projected, err := RecordToMap(model, WithProjectionIncludeNilPointers(false))
	require.NoError(t, err)
	assert.NotContains(t, projected, "enabled")
}

func TestMapToRecord_AndApplyMapPatch(t *testing.T) {
	payload := map[string]any{
		"id":      uuid.New().String(),
		"name":    "Original",
		"count":   12,
		"enabled": true,
	}

	record, err := MapToRecord[*mapHelperModel](payload)
	require.NoError(t, err)
	require.NotNil(t, record)
	assert.Equal(t, "Original", record.Name)
	assert.Equal(t, 12, record.Count)
	require.NotNil(t, record.Enabled)
	assert.True(t, *record.Enabled)

	updated, columns, err := ApplyMapPatch(record, map[string]any{
		"name":    "Updated",
		"count":   0,
		"enabled": false,
	}, WithPatchAllowedFields("name", "count", "enabled"))
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"name", "count", "enabled"}, columns)
	assert.Equal(t, "Updated", updated.Name)
	assert.Equal(t, 0, updated.Count)
	require.NotNil(t, updated.Enabled)
	assert.False(t, *updated.Enabled)
}

func TestApplyMapPatch_UnknownAndAllowlistErrors(t *testing.T) {
	record := &mapHelperModel{ID: uuid.New(), Name: "Before"}

	_, _, err := ApplyMapPatch(record, map[string]any{
		"unknown": "value",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnknownPatchField)

	_, _, err = ApplyMapPatch(record, map[string]any{
		"name": "After",
	}, WithPatchAllowedFields("count"))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrPatchFieldNotAllowed)
}

func TestApplyMapPatch_DenyPrimaryKey(t *testing.T) {
	record := &mapHelperModel{ID: uuid.New(), Name: "Before"}

	_, _, err := ApplyMapPatch(record, map[string]any{
		"id": uuid.New().String(),
	}, WithPatchDenyPrimaryKey())
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrPatchPrimaryKeyNotAllowed)
}

func TestUpdateCriteriaForMapPatch(t *testing.T) {
	criteria, err := UpdateCriteriaForMapPatch(map[string]any{
		"name":  "Updated Name",
		"count": 7,
	}, WithPatchAllowedFields("name", "count"))
	require.NoError(t, err)
	require.Len(t, criteria, 3) // UpdateColumns + two SetColumn criteria
}

func TestUpdateCriteriaForMapPatch_WithRepositoryUpdate(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	repo := newTestUserRepository(db)

	record := &TestUser{
		ID:        uuid.New(),
		Name:      "Before",
		Email:     "criteria@example.com",
		CompanyID: uuid.New(),
	}
	created, err := repo.Create(ctx, record)
	require.NoError(t, err)

	criteria, err := UpdateCriteriaForMapPatch(
		map[string]any{"name": "After"},
		WithPatchAllowedFields("name"),
	)
	require.NoError(t, err)
	criteria = append(criteria, UpdateByID(created.ID.String()))

	updated, err := repo.Update(ctx, created, criteria...)
	require.NoError(t, err)
	assert.Equal(t, "After", updated.Name)
	assert.Equal(t, created.Email, updated.Email)
}

func TestUpdateByIDWithMapPatch(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	repo := newTestUserRepository(db)

	user := &TestUser{
		ID:        uuid.New(),
		Name:      "Before",
		Email:     "before@example.com",
		CompanyID: uuid.New(),
	}

	created, err := repo.Create(ctx, user)
	require.NoError(t, err)

	updated, err := UpdateByIDWithMapPatch(
		ctx,
		repo,
		created.ID.String(),
		map[string]any{"name": "After"},
		[]UpdateCriteria{UpdateByID(created.ID.String())},
		WithPatchAllowedFields("name"),
	)
	require.NoError(t, err)
	assert.Equal(t, "After", updated.Name)
	assert.Equal(t, created.Email, updated.Email)
}

func TestUpdateByIDWithMapPatch_DetectsNotFound(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	repo := newTestUserRepository(db)
	unknown := uuid.New().String()

	_, err := UpdateByIDWithMapPatch(
		ctx,
		repo,
		unknown,
		map[string]any{"name": "After"},
		nil,
		WithPatchAllowedFields("name"),
	)
	require.Error(t, err)
	assert.True(t, IsRecordNotFound(err))
}

func TestRecordNotFoundSentinelSupportsErrorsIs(t *testing.T) {
	err := NewRecordNotFound()
	assert.True(t, stderrors.Is(err, ErrRecordNotFound))
	assert.True(t, IsRecordNotFound(err))
}

func TestApplyMapPatch_RejectsOverflowAndUnderflow(t *testing.T) {
	_, _, err := ApplyMapPatch(&mapHelperModel{}, map[string]any{
		"count": uint64(math.MaxUint64),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "overflows int64")

	_, _, err = ApplyMapPatch(&mapHelperUnsignedModel{}, map[string]any{
		"value": int64(-1),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "underflows uint64")
}
