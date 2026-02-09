package repository

import (
	"context"
	stderrors "errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

// MapKeyMode controls which field name strategy is used for map payloads.
type MapKeyMode string

const (
	// MapKeyBun uses Bun column names (`bun:"column_name"` or Bun's inferred snake_case name).
	MapKeyBun MapKeyMode = "bun"
	// MapKeyJSON uses JSON tags (`json:"field_name"`) with Go field name fallback.
	MapKeyJSON MapKeyMode = "json"
	// MapKeyStruct uses the struct field name (e.g. `FieldName`).
	MapKeyStruct MapKeyMode = "struct"
)

var (
	// ErrUnknownPatchField indicates that a patch payload includes a field that does not exist.
	ErrUnknownPatchField = stderrors.New("repository: unknown patch field")
	// ErrPatchFieldNotAllowed indicates that a patch payload attempted to update a disallowed field.
	ErrPatchFieldNotAllowed = stderrors.New("repository: patch field not allowed")
	// ErrPatchPrimaryKeyNotAllowed indicates a patch payload attempted to update a primary key field.
	ErrPatchPrimaryKeyNotAllowed = stderrors.New("repository: patch primary key not allowed")
)

type mapProjectionConfig struct {
	keyMode            MapKeyMode
	includeNilPointers bool
}

func defaultMapProjectionConfig() mapProjectionConfig {
	return mapProjectionConfig{
		keyMode:            MapKeyBun,
		includeNilPointers: true,
	}
}

// MapProjectionOption configures RecordToMap/EntityToMap projection behavior.
type MapProjectionOption func(*mapProjectionConfig)

// WithProjectionKeyMode selects how projected map keys are generated.
func WithProjectionKeyMode(mode MapKeyMode) MapProjectionOption {
	return func(cfg *mapProjectionConfig) {
		cfg.keyMode = normalizeMapKeyMode(mode)
	}
}

// WithProjectionIncludeNilPointers controls whether nil pointers are included as map entries.
func WithProjectionIncludeNilPointers(include bool) MapProjectionOption {
	return func(cfg *mapProjectionConfig) {
		cfg.includeNilPointers = include
	}
}

type mapPatchConfig struct {
	keyMode        MapKeyMode
	allowedFields  map[string]struct{}
	ignoreUnknown  bool
	ignoreNil      bool
	denyPrimaryKey bool
}

func defaultMapPatchConfig() mapPatchConfig {
	return mapPatchConfig{
		keyMode:       MapKeyBun,
		ignoreUnknown: false,
		ignoreNil:     false,
	}
}

// MapPatchOption configures map patch behavior.
type MapPatchOption func(*mapPatchConfig)

// WithPatchKeyMode selects how payload keys are resolved.
func WithPatchKeyMode(mode MapKeyMode) MapPatchOption {
	return func(cfg *mapPatchConfig) {
		cfg.keyMode = normalizeMapKeyMode(mode)
	}
}

// WithPatchAllowedFields allowlists patchable fields. Values can be Bun names, JSON names, or struct field names.
func WithPatchAllowedFields(fields ...string) MapPatchOption {
	return func(cfg *mapPatchConfig) {
		if cfg.allowedFields == nil {
			cfg.allowedFields = make(map[string]struct{}, len(fields))
		}
		for _, field := range fields {
			field = strings.TrimSpace(field)
			if field == "" {
				continue
			}
			cfg.allowedFields[field] = struct{}{}
		}
	}
}

// WithPatchIgnoreUnknown controls whether unknown payload keys are ignored.
func WithPatchIgnoreUnknown(ignore bool) MapPatchOption {
	return func(cfg *mapPatchConfig) {
		cfg.ignoreUnknown = ignore
	}
}

// WithPatchIgnoreNil controls whether nil payload values are ignored.
func WithPatchIgnoreNil(ignore bool) MapPatchOption {
	return func(cfg *mapPatchConfig) {
		cfg.ignoreNil = ignore
	}
}

// WithPatchDenyPrimaryKey rejects updates to primary key fields.
func WithPatchDenyPrimaryKey() MapPatchOption {
	return func(cfg *mapPatchConfig) {
		cfg.denyPrimaryKey = true
	}
}

// RecordToMap projects a typed record into a map using map-native reflection (no JSON roundtrip).
func RecordToMap[T any](record T, opts ...MapProjectionOption) (map[string]any, error) {
	return EntityToMap(record, opts...)
}

// EntityToMap projects an entity into a map using map-native reflection (no JSON roundtrip).
func EntityToMap(entity any, opts ...MapProjectionOption) (map[string]any, error) {
	cfg := defaultMapProjectionConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	structValue, err := readStructValue(entity)
	if err != nil {
		return nil, err
	}

	desc, err := getMapModelDescriptor(structValue.Type())
	if err != nil {
		return nil, err
	}

	out := make(map[string]any, len(desc.fields))
	for i := range desc.fields {
		field := desc.fields[i]
		key := field.key(cfg.keyMode)
		if key == "" {
			continue
		}

		fv, ok := fieldByIndexForRead(structValue, field.index)
		if !ok {
			if cfg.includeNilPointers {
				out[key] = nil
			}
			continue
		}

		value, include := projectedFieldValue(fv, cfg.includeNilPointers)
		if !include {
			continue
		}
		out[key] = value
	}

	return out, nil
}

// MapToRecord maps a payload into a record using map-native reflection (no JSON roundtrip).
func MapToRecord[T any](payload map[string]any, opts ...MapPatchOption) (T, error) {
	var zero T
	record, _, err := ApplyMapPatch(zero, payload, opts...)
	if err != nil {
		return zero, err
	}
	return record, nil
}

// ApplyMapPatch applies a partial map payload onto a record and returns the updated record
// plus the changed Bun column names.
func ApplyMapPatch[T any](record T, patch map[string]any, opts ...MapPatchOption) (T, []string, error) {
	var zero T

	cfg := defaultMapPatchConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	if len(patch) == 0 {
		return record, nil, nil
	}

	structValue, finalize, err := mutableStructValue(record)
	if err != nil {
		return zero, nil, err
	}

	desc, err := getMapModelDescriptor(structValue.Type())
	if err != nil {
		return zero, nil, err
	}

	plan, err := buildPatchPlan(desc, patch, cfg)
	if err != nil {
		return zero, nil, err
	}

	columns := make([]string, 0, len(plan))
	seenColumns := make(map[string]struct{}, len(plan))

	for _, item := range plan {
		fieldValue, err := fieldByIndexForWrite(structValue, item.field.index)
		if err != nil {
			return zero, nil, err
		}
		if err := assignValue(fieldValue, item.value); err != nil {
			return zero, nil, fmt.Errorf("repository: patch field %q (%s): %w", item.inputKey, item.field.bunName, err)
		}
		if _, exists := seenColumns[item.field.bunName]; !exists {
			seenColumns[item.field.bunName] = struct{}{}
			columns = append(columns, item.field.bunName)
		}
	}

	return finalize(), columns, nil
}

// UpdateCriteriaForMapPatch builds UpdateCriteria for direct query patch updates.
// It returns UpdateColumns(...) plus UpdateSetColumn(...) entries.
func UpdateCriteriaForMapPatch(patch map[string]any, opts ...MapPatchOption) ([]UpdateCriteria, error) {
	cfg := defaultMapPatchConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	if len(patch) == 0 {
		return nil, nil
	}

	plan, err := buildRawPatchPlan(patch, cfg)
	if err != nil {
		return nil, err
	}

	if len(plan) == 0 {
		return nil, nil
	}

	columns := make([]string, 0, len(plan))
	criteria := make([]UpdateCriteria, 0, len(plan)+1)

	for _, item := range plan {
		columns = append(columns, item.column)
		criteria = append(criteria, UpdateSetColumn(item.column, item.value))
	}

	criteria = append([]UpdateCriteria{UpdateColumns(columns...)}, criteria...)
	return criteria, nil
}

// UpdateByIDWithMapPatch performs an ID-based safe partial update flow:
// load current record -> apply map patch -> update changed columns only.
func UpdateByIDWithMapPatch[T any](
	ctx context.Context,
	repo Repository[T],
	id string,
	patch map[string]any,
	updateCriteria []UpdateCriteria,
	opts ...MapPatchOption,
) (T, error) {
	var zero T

	current, err := repo.GetByID(ctx, id)
	if err != nil {
		return zero, err
	}

	effectiveOpts := append([]MapPatchOption{}, opts...)
	effectiveOpts = append(effectiveOpts, WithPatchDenyPrimaryKey())

	patched, columns, err := ApplyMapPatch(current, patch, effectiveOpts...)
	if err != nil {
		return zero, err
	}
	if len(columns) == 0 {
		return current, nil
	}

	criteria := make([]UpdateCriteria, 0, len(updateCriteria)+1)
	criteria = append(criteria, updateCriteria...)
	criteria = append(criteria, UpdateColumns(columns...))

	return repo.Update(ctx, patched, criteria...)
}

// UpdateByIDWithMapPatchTx is the transactional variant of UpdateByIDWithMapPatch.
func UpdateByIDWithMapPatchTx[T any](
	ctx context.Context,
	repo Repository[T],
	tx bun.IDB,
	id string,
	patch map[string]any,
	updateCriteria []UpdateCriteria,
	opts ...MapPatchOption,
) (T, error) {
	var zero T

	current, err := repo.GetByIDTx(ctx, tx, id)
	if err != nil {
		return zero, err
	}

	effectiveOpts := append([]MapPatchOption{}, opts...)
	effectiveOpts = append(effectiveOpts, WithPatchDenyPrimaryKey())

	patched, columns, err := ApplyMapPatch(current, patch, effectiveOpts...)
	if err != nil {
		return zero, err
	}
	if len(columns) == 0 {
		return current, nil
	}

	criteria := make([]UpdateCriteria, 0, len(updateCriteria)+1)
	criteria = append(criteria, updateCriteria...)
	criteria = append(criteria, UpdateColumns(columns...))

	return repo.UpdateTx(ctx, tx, patched, criteria...)
}

// MapRecordMapper is a reusable generic utility set for map-based integrations.
type MapRecordMapper[T any] struct {
	ToRecord   func(map[string]any) (T, error)
	ToMap      func(T) (map[string]any, error)
	ApplyPatch func(T, map[string]any) (T, []string, error)
}

// MapRecordMapperConfig configures NewMapRecordMapper.
type MapRecordMapperConfig struct {
	ProjectionOptions []MapProjectionOption
	PatchOptions      []MapPatchOption
}

// NewMapRecordMapper returns a reusable map-native generic adapter utility set.
func NewMapRecordMapper[T any](cfg MapRecordMapperConfig) MapRecordMapper[T] {
	projectionOpts := append([]MapProjectionOption{}, cfg.ProjectionOptions...)
	patchOpts := append([]MapPatchOption{}, cfg.PatchOptions...)

	return MapRecordMapper[T]{
		ToRecord: func(payload map[string]any) (T, error) {
			return MapToRecord[T](payload, patchOpts...)
		},
		ToMap: func(record T) (map[string]any, error) {
			return RecordToMap(record, projectionOpts...)
		},
		ApplyPatch: func(record T, patch map[string]any) (T, []string, error) {
			return ApplyMapPatch(record, patch, patchOpts...)
		},
	}
}

type patchPlanItem struct {
	inputKey string
	column   string
	value    any
	field    mapFieldBinding
}

type mapModelDescriptor struct {
	fields   []mapFieldBinding
	byBun    map[string]mapFieldBinding
	byJSON   map[string]mapFieldBinding
	byStruct map[string]mapFieldBinding
}

type mapFieldBinding struct {
	index       []int
	structName  string
	bunName     string
	jsonName    string
	jsonIgnored bool
	isPrimary   bool
}

func (f mapFieldBinding) key(mode MapKeyMode) string {
	switch normalizeMapKeyMode(mode) {
	case MapKeyJSON:
		if f.jsonIgnored {
			return ""
		}
		return f.jsonName
	case MapKeyStruct:
		return f.structName
	default:
		return f.bunName
	}
}

var mapModelDescriptorCache sync.Map // map[reflect.Type]*mapModelDescriptor

func getMapModelDescriptor(typ reflect.Type) (*mapModelDescriptor, error) {
	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("repository: expected struct type, got %s", typ)
	}

	if cached, ok := mapModelDescriptorCache.Load(typ); ok {
		if desc, ok := cached.(*mapModelDescriptor); ok {
			return desc, nil
		}
	}

	desc, err := buildMapModelDescriptor(typ)
	if err != nil {
		return nil, err
	}
	mapModelDescriptorCache.Store(typ, desc)
	return desc, nil
}

func buildMapModelDescriptor(typ reflect.Type) (*mapModelDescriptor, error) {
	desc := &mapModelDescriptor{
		byBun:    make(map[string]mapFieldBinding),
		byJSON:   make(map[string]mapFieldBinding),
		byStruct: make(map[string]mapFieldBinding),
	}

	fields, err := collectMapFieldBindings(typ, nil)
	if err != nil {
		return nil, err
	}
	desc.fields = fields

	for _, field := range fields {
		if err := descriptorAddField(desc.byStruct, field.structName, field); err != nil {
			return nil, err
		}
		if err := descriptorAddField(desc.byBun, field.bunName, field); err != nil {
			return nil, err
		}
		if !field.jsonIgnored && field.jsonName != "" {
			if err := descriptorAddField(desc.byJSON, field.jsonName, field); err != nil {
				return nil, err
			}
		}
	}

	return desc, nil
}

func descriptorAddField(target map[string]mapFieldBinding, key string, field mapFieldBinding) error {
	if key == "" {
		return nil
	}
	if existing, ok := target[key]; ok {
		return fmt.Errorf(
			"repository: duplicate map key %q for fields %s and %s",
			key,
			existing.structName,
			field.structName,
		)
	}
	target[key] = field
	return nil
}

func collectMapFieldBindings(typ reflect.Type, prefix []int) ([]mapFieldBinding, error) {
	var result []mapFieldBinding

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)

		if !field.IsExported() {
			continue
		}

		index := appendIndex(prefix, i)
		if shouldSkipMapField(field) {
			continue
		}

		if shouldInlineMapField(field) {
			inlineType := field.Type
			if inlineType.Kind() == reflect.Ptr {
				inlineType = inlineType.Elem()
			}
			nested, err := collectMapFieldBindings(inlineType, index)
			if err != nil {
				return nil, err
			}
			result = append(result, nested...)
			continue
		}

		bunName, bunSkip, isPrimary := parseBunFieldName(field)
		if bunSkip {
			continue
		}

		jsonName, jsonIgnored := parseJSONFieldName(field)
		result = append(result, mapFieldBinding{
			index:       index,
			structName:  field.Name,
			bunName:     bunName,
			jsonName:    jsonName,
			jsonIgnored: jsonIgnored,
			isPrimary:   isPrimary,
		})
	}

	return result, nil
}

func buildPatchPlan(desc *mapModelDescriptor, patch map[string]any, cfg mapPatchConfig) ([]patchPlanItem, error) {
	keyLookup, err := descriptorLookupByMode(desc, cfg.keyMode)
	if err != nil {
		return nil, err
	}

	keys := sortedMapKeys(patch)
	plan := make([]patchPlanItem, 0, len(keys))

	for _, key := range keys {
		value := patch[key]
		if cfg.ignoreNil && value == nil {
			continue
		}

		field, ok := keyLookup[key]
		if !ok {
			if cfg.ignoreUnknown {
				continue
			}
			return nil, fmt.Errorf("%w: %s", ErrUnknownPatchField, key)
		}

		if cfg.denyPrimaryKey && field.isPrimary {
			return nil, fmt.Errorf("%w: %s", ErrPatchPrimaryKeyNotAllowed, key)
		}

		if !fieldAllowed(cfg.allowedFields, key, field) {
			return nil, fmt.Errorf("%w: %s", ErrPatchFieldNotAllowed, key)
		}

		plan = append(plan, patchPlanItem{
			inputKey: key,
			column:   field.bunName,
			value:    value,
			field:    field,
		})
	}

	return plan, nil
}

func buildRawPatchPlan(patch map[string]any, cfg mapPatchConfig) ([]patchPlanItem, error) {
	if cfg.keyMode != MapKeyBun {
		return nil, fmt.Errorf(
			"repository: UpdateCriteriaForMapPatch requires %q key mode, got %q",
			MapKeyBun,
			cfg.keyMode,
		)
	}

	keys := sortedMapKeys(patch)
	plan := make([]patchPlanItem, 0, len(keys))
	for _, key := range keys {
		value := patch[key]
		if cfg.ignoreNil && value == nil {
			continue
		}

		if cfg.denyPrimaryKey && strings.EqualFold(key, "id") {
			return nil, fmt.Errorf("%w: %s", ErrPatchPrimaryKeyNotAllowed, key)
		}
		if !fieldAllowedRaw(cfg.allowedFields, key) {
			return nil, fmt.Errorf("%w: %s", ErrPatchFieldNotAllowed, key)
		}

		plan = append(plan, patchPlanItem{
			inputKey: key,
			column:   key,
			value:    value,
		})
	}
	return plan, nil
}

func descriptorLookupByMode(desc *mapModelDescriptor, mode MapKeyMode) (map[string]mapFieldBinding, error) {
	switch normalizeMapKeyMode(mode) {
	case MapKeyJSON:
		return desc.byJSON, nil
	case MapKeyStruct:
		return desc.byStruct, nil
	case MapKeyBun:
		return desc.byBun, nil
	default:
		return nil, fmt.Errorf("repository: unsupported map key mode %q", mode)
	}
}

func readStructValue(entity any) (reflect.Value, error) {
	value := reflect.ValueOf(entity)
	if !value.IsValid() {
		return reflect.Value{}, fmt.Errorf("repository: nil entity")
	}

	for value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return reflect.Value{}, fmt.Errorf("repository: nil entity pointer")
		}
		value = value.Elem()
	}

	if value.Kind() != reflect.Struct {
		return reflect.Value{}, fmt.Errorf("repository: expected struct or struct pointer, got %s", value.Kind())
	}

	return value, nil
}

func mutableStructValue[T any](record T) (reflect.Value, func() T, error) {
	value := reflect.ValueOf(record)
	if !value.IsValid() {
		return reflect.Value{}, nil, fmt.Errorf("repository: invalid record")
	}

	if value.Kind() == reflect.Ptr {
		if value.IsNil() {
			value = reflect.New(value.Type().Elem())
		}
		if value.Elem().Kind() != reflect.Struct {
			return reflect.Value{}, nil, fmt.Errorf("repository: expected pointer to struct, got %s", value.Elem().Kind())
		}
		finalize := func() T {
			return value.Interface().(T)
		}
		return value.Elem(), finalize, nil
	}

	if value.Kind() != reflect.Struct {
		return reflect.Value{}, nil, fmt.Errorf("repository: expected struct or struct pointer, got %s", value.Kind())
	}

	clone := reflect.New(value.Type())
	clone.Elem().Set(value)
	finalize := func() T {
		return clone.Elem().Interface().(T)
	}
	return clone.Elem(), finalize, nil
}

func fieldByIndexForRead(structValue reflect.Value, index []int) (reflect.Value, bool) {
	current := structValue
	for _, idx := range index {
		if current.Kind() == reflect.Ptr {
			if current.IsNil() {
				return reflect.Value{}, false
			}
			current = current.Elem()
		}
		if current.Kind() != reflect.Struct {
			return reflect.Value{}, false
		}
		current = current.Field(idx)
	}
	return current, true
}

func fieldByIndexForWrite(structValue reflect.Value, index []int) (reflect.Value, error) {
	current := structValue
	for _, idx := range index {
		if current.Kind() == reflect.Ptr {
			if current.IsNil() {
				current.Set(reflect.New(current.Type().Elem()))
			}
			current = current.Elem()
		}
		if current.Kind() != reflect.Struct {
			return reflect.Value{}, fmt.Errorf("repository: invalid write path for index %v", index)
		}
		current = current.Field(idx)
	}
	if !current.CanSet() {
		return reflect.Value{}, fmt.Errorf("repository: field at index %v is not settable", index)
	}
	return current, nil
}

func projectedFieldValue(value reflect.Value, includeNilPointers bool) (any, bool) {
	if !value.IsValid() {
		return nil, false
	}
	if value.Kind() == reflect.Ptr {
		if value.IsNil() {
			if includeNilPointers {
				return nil, true
			}
			return nil, false
		}
		return value.Elem().Interface(), true
	}
	return value.Interface(), true
}

func assignValue(dst reflect.Value, src any) error {
	if !dst.CanSet() {
		return fmt.Errorf("destination is not settable")
	}

	if src == nil {
		setNilOrZero(dst)
		return nil
	}

	if dst.Kind() == reflect.Ptr {
		if dst.IsNil() {
			dst.Set(reflect.New(dst.Type().Elem()))
		}
		return assignValue(dst.Elem(), src)
	}

	if dst.Type() == reflect.TypeOf(uuid.UUID{}) {
		parsed, err := parseUUIDValue(src)
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(parsed))
		return nil
	}

	if dst.Type() == reflect.TypeOf(time.Time{}) {
		parsed, err := parseTimeValue(src)
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(parsed))
		return nil
	}

	srcValue := reflect.ValueOf(src)
	if srcValue.IsValid() {
		if srcValue.Type().AssignableTo(dst.Type()) {
			dst.Set(srcValue)
			return nil
		}
		if srcValue.Type().ConvertibleTo(dst.Type()) {
			dst.Set(srcValue.Convert(dst.Type()))
			return nil
		}
	}

	switch dst.Kind() {
	case reflect.String:
		dst.SetString(fmt.Sprint(src))
		return nil
	case reflect.Bool:
		v, err := toBool(src)
		if err != nil {
			return err
		}
		dst.SetBool(v)
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v, err := toInt64(src)
		if err != nil {
			return err
		}
		dst.SetInt(v)
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		v, err := toUint64(src)
		if err != nil {
			return err
		}
		dst.SetUint(v)
		return nil
	case reflect.Float32, reflect.Float64:
		v, err := toFloat64(src)
		if err != nil {
			return err
		}
		dst.SetFloat(v)
		return nil
	case reflect.Interface:
		dst.Set(reflect.ValueOf(src))
		return nil
	}

	return fmt.Errorf("cannot assign %T to %s", src, dst.Type())
}

func parseUUIDValue(src any) (uuid.UUID, error) {
	switch v := src.(type) {
	case uuid.UUID:
		return v, nil
	case string:
		id, err := uuid.Parse(v)
		if err != nil {
			return uuid.Nil, fmt.Errorf("invalid uuid value %q: %w", v, err)
		}
		return id, nil
	default:
		return uuid.Nil, fmt.Errorf("unsupported uuid source type %T", src)
	}
}

func parseTimeValue(src any) (time.Time, error) {
	switch v := src.(type) {
	case time.Time:
		return v, nil
	case string:
		for _, layout := range []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02 15:04:05",
			"2006-01-02",
		} {
			if parsed, err := time.Parse(layout, v); err == nil {
				return parsed, nil
			}
		}
		return time.Time{}, fmt.Errorf("invalid time value %q", v)
	default:
		return time.Time{}, fmt.Errorf("unsupported time source type %T", src)
	}
}

func setNilOrZero(dst reflect.Value) {
	switch dst.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Interface:
		dst.SetZero()
	default:
		dst.Set(reflect.Zero(dst.Type()))
	}
}

func parseBunFieldName(field reflect.StructField) (string, bool, bool) {
	tag := strings.TrimSpace(field.Tag.Get("bun"))
	if tag == "-" {
		return "", true, false
	}

	parts := strings.Split(tag, ",")
	name := ""
	isPrimary := false
	for i, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if i == 0 && part == "-" {
			return "", true, false
		}
		if i == 0 && !strings.Contains(part, ":") && !isBunTagOption(part) {
			name = part
			continue
		}
		if part == "pk" {
			isPrimary = true
		}
	}

	if name == "" {
		name = toSnakeCase(field.Name)
	}
	return name, false, isPrimary
}

func parseJSONFieldName(field reflect.StructField) (string, bool) {
	tag := strings.TrimSpace(field.Tag.Get("json"))
	if tag == "-" {
		return "", true
	}
	if tag == "" {
		return field.Name, false
	}

	parts := strings.Split(tag, ",")
	name := strings.TrimSpace(parts[0])
	switch name {
	case "-":
		return "", true
	case "":
		return field.Name, false
	default:
		return name, false
	}
}

func isBunTagOption(part string) bool {
	switch part {
	case "pk", "autoincrement", "notnull", "nullzero", "unique", "scanonly", "soft_delete", "skipupdate", "skipinsert":
		return true
	default:
		return false
	}
}

func shouldSkipMapField(field reflect.StructField) bool {
	if field.Name == "BaseModel" {
		return true
	}
	return false
}

func shouldInlineMapField(field reflect.StructField) bool {
	if !field.Anonymous {
		return false
	}
	typ := field.Type
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return false
	}
	if typ == reflect.TypeOf(time.Time{}) {
		return false
	}
	return true
}

func fieldAllowed(allowlist map[string]struct{}, payloadKey string, field mapFieldBinding) bool {
	if len(allowlist) == 0 {
		return true
	}
	if _, ok := allowlist[payloadKey]; ok {
		return true
	}
	if _, ok := allowlist[field.bunName]; ok {
		return true
	}
	if _, ok := allowlist[field.structName]; ok {
		return true
	}
	if !field.jsonIgnored {
		if _, ok := allowlist[field.jsonName]; ok {
			return true
		}
	}
	return false
}

func fieldAllowedRaw(allowlist map[string]struct{}, payloadKey string) bool {
	if len(allowlist) == 0 {
		return true
	}
	_, ok := allowlist[payloadKey]
	return ok
}

func sortedMapKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func appendIndex(prefix []int, i int) []int {
	index := make([]int, len(prefix)+1)
	copy(index, prefix)
	index[len(prefix)] = i
	return index
}

func normalizeMapKeyMode(mode MapKeyMode) MapKeyMode {
	switch mode {
	case MapKeyJSON:
		return MapKeyJSON
	case MapKeyStruct:
		return MapKeyStruct
	case MapKeyBun, "":
		return MapKeyBun
	default:
		return MapKeyBun
	}
}

func toSnakeCase(value string) string {
	if value == "" {
		return value
	}

	var b strings.Builder
	runes := []rune(value)
	for i, r := range runes {
		if unicode.IsUpper(r) {
			if i > 0 {
				prev := runes[i-1]
				if unicode.IsLower(prev) || unicode.IsDigit(prev) {
					b.WriteByte('_')
				} else if i+1 < len(runes) && unicode.IsLower(runes[i+1]) {
					b.WriteByte('_')
				}
			}
			b.WriteRune(unicode.ToLower(r))
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

func toBool(src any) (bool, error) {
	switch v := src.(type) {
	case bool:
		return v, nil
	case string:
		parsed, err := strconv.ParseBool(v)
		if err != nil {
			return false, fmt.Errorf("invalid bool value %q", v)
		}
		return parsed, nil
	default:
		return false, fmt.Errorf("unsupported bool source type %T", src)
	}
}

func toInt64(src any) (int64, error) {
	switch v := src.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case string:
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid int value %q", v)
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("unsupported int source type %T", src)
	}
}

func toUint64(src any) (uint64, error) {
	switch v := src.(type) {
	case int:
		return uint64(v), nil
	case int8:
		return uint64(v), nil
	case int16:
		return uint64(v), nil
	case int32:
		return uint64(v), nil
	case int64:
		return uint64(v), nil
	case uint:
		return uint64(v), nil
	case uint8:
		return uint64(v), nil
	case uint16:
		return uint64(v), nil
	case uint32:
		return uint64(v), nil
	case uint64:
		return v, nil
	case float32:
		return uint64(v), nil
	case float64:
		return uint64(v), nil
	case string:
		parsed, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid uint value %q", v)
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("unsupported uint source type %T", src)
	}
}

func toFloat64(src any) (float64, error) {
	switch v := src.(type) {
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case string:
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid float value %q", v)
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("unsupported float source type %T", src)
	}
}
