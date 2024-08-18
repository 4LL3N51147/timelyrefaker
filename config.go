package main

type Config struct {
	NumRows       int64    `yaml:"num_rows"`
	Schema        Schema   `yaml:"schema"`
	TableName     string   `yaml:"table_name"`
	WriteBatchNum int      `yaml:"write_batch_num"`
	PreQueries    []string `yaml:"pre_queries"`
	PostQueries   []string `yaml:"post_queries"`
}

type ValueRange struct {
	Start any `yaml:"start"`
	End   any `yaml:"end"`
}

type FieldSchema struct {
	Name     string      `yaml:"name"`
	Count    int         `yaml:"count"`
	Nullable float64     `yaml:"nullable"`
	Range    *ValueRange `yaml:"range"`
	Type     string      `yaml:"-"`
}

type Schema struct {
	StorageFormat string                   `yaml:"storage_format"`
	Fields        map[string][]FieldSchema `yaml:"fields"`
	TableProps    map[string]string        `yaml:"table_props"`
}
