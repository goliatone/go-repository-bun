package repository

import (
	"reflect"
	"strings"

	"github.com/uptrace/bun"
)

type ModelField struct {
	Name       string `json:"name"`
	IsPK       bool   `json:"is_pk"`
	SQLType    string `json:"sql_type"`
	SQLDefault string `json:"sql_default"`
	Identity   bool   `json:"identity"`
	IsUnique   bool   `json:"is_unique"`
	// SQLName    string `json:"sql_name"`
}

// GetModelFields returns a list of fields for the model:
// fields := GetModelFields(db, &User{})
func GetModelFields(db *bun.DB, model any) []ModelField {
	table := db.Table(reflect.TypeOf(model))
	var fields []ModelField

	for _, field := range table.Fields {
		fields = append(fields, ModelField{
			Name:       field.Name,
			IsPK:       field.IsPK,
			SQLType:    field.UserSQLType,
			SQLDefault: field.SQLDefault,
			Identity:   field.Identity,
			IsUnique:   field.Tag.HasOption("unique") || field.Tag.HasOption("pk"),
			// SQLName:    string(field.SQLName),
		})
	}

	return fields
}

// ModelMeta represents the collected metadata for a model
type ModelMeta struct {
	TableName string      `json:"table_name"`
	Fields    []FieldMeta `json:"fields"`
}

// FieldMeta represents metadata for a single field
type FieldMeta struct {
	Name         string   `json:"name"`
	StructName   string   `json:"struct_name"`
	Type         string   `json:"type"`
	IsRequired   bool     `json:"is_required"`
	IsNullable   bool     `json:"is_nullable"`
	IsUnique     bool     `json:"is_unique"`
	IsPK         bool     `json:"is_pk"`
	Description  string   `json:"description"`
	DefaultValue string   `json:"default_value"`
	Validations  []string `json:"validations,omitempty"`
}

// GenerateModelMeta generates metadata from a model using reflection
func GenerateModelMeta(model any) ModelMeta {
	typ := reflect.TypeOf(model)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	meta := ModelMeta{
		TableName: getTableName(model),
		Fields:    make([]FieldMeta, 0),
	}

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		if field.Name == "BaseModel" {
			bunTag := field.Tag.Get("bun")
			parts := strings.Split(bunTag, ",")
			for _, part := range parts {
				if name, ok := strings.CutPrefix(part, ":table"); ok && name != "" {
					meta.TableName = name
					break
				}
			}
			continue
		}

		bunTag := field.Tag.Get("bun")
		jsonTag := field.Tag.Get("json")

		fieldMeta := FieldMeta{
			StructName: field.Name,
			Name:       getJSONName(jsonTag),
			Type:       getFieldType(field.Type),
		}

		// Parse bun tags
		if bunTag != "" {
			parseBunTag(&fieldMeta, bunTag)
		}

		// Parse validation tags if present
		if validateTag := field.Tag.Get("validate"); validateTag != "" {
			fieldMeta.Validations = strings.Split(validateTag, ",")
			fieldMeta.IsRequired = contains(fieldMeta.Validations, "required")
		}

		meta.Fields = append(meta.Fields, fieldMeta)
	}

	return meta
}

func getTableName(model any) string {
	if table, ok := model.(interface {
		TableName() string
	}); ok {
		return table.TableName()
	}

	typ := reflect.TypeOf(model)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	return strings.ToLower(typ.Name())
}

func getJSONName(jsonTag string) string {
	if jsonTag == "" {
		return ""
	}
	parts := strings.Split(jsonTag, ",")
	if parts[0] == "-" {
		return ""
	}
	return parts[0]
}

func getFieldType(t reflect.Type) string {
	switch t.Kind() {
	case reflect.Ptr:
		return getFieldType(t.Elem())
	case reflect.Slice:
		return "array:" + getFieldType(t.Elem())
	default:
		return t.String()
	}
}

func parseBunTag(field *FieldMeta, tag string) {
	parts := strings.Split(tag, ",")
	for _, part := range parts {
		switch {
		case part == "unique":
			field.IsUnique = true
		case part == "pk":
			field.IsPK = true
			field.IsUnique = true
		case part == "null":
			field.IsNullable = true
		case strings.HasPrefix(part, "default:"):
			field.DefaultValue = strings.TrimPrefix(part, "default:")
		}
	}
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
