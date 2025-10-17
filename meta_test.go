package repository

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uptrace/bun"
)

type metaTestModel struct {
	bun.BaseModel `bun:"table:meta_models,alias:mm"`

	ID          int `bun:"id,pk"`
	DisplayName string
	Hidden      string `json:"-"`
	Count       int    `json:",omitempty"`
}

func TestGenerateModelMeta_UsesTableNameFromBaseModel(t *testing.T) {
	meta := GenerateModelMeta(metaTestModel{})

	assert.Equal(t, "meta_models", meta.TableName)
	assert.Len(t, meta.Fields, 4)

	fieldNames := map[string]FieldMeta{}
	for _, f := range meta.Fields {
		fieldNames[f.StructName] = f
	}

	assert.Equal(t, "ID", fieldNames["ID"].Name)
	assert.Equal(t, "DisplayName", fieldNames["DisplayName"].Name)
	assert.Equal(t, "Count", fieldNames["Count"].Name)
	assert.Equal(t, "", fieldNames["Hidden"].Name)
}
