package main

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

type Generator struct {
	numRows       int64
	tableName     string
	batchNum      int
	storageFormat string
	tableProps    map[string]string
	fieldDefs     []FieldSchema

	preQueries  []string
	postQueries []string
}

func NewGenerator(config Config) (*Generator, error) {
	fieldDefs := make([]FieldSchema, 0)

	for typ, fSchemas := range config.Schema.Fields {
		for _, fSchema := range fSchemas {
			if fSchema.Count != 0 {
				for i := 0; i < fSchema.Count; i++ {
					colName := fmt.Sprintf("col_%s%d", typ, i)
					fSchema.Name = colName
					fSchema.Type = typ
					fieldDefs = append(fieldDefs, fSchema)
				}
			} else {
				fSchema.Type = typ
				fieldDefs = append(fieldDefs, fSchema)
			}
		}
	}

	return &Generator{
		numRows:       config.NumRows,
		tableName:     config.TableName,
		batchNum:      config.WriteBatchNum,
		storageFormat: config.Schema.StorageFormat,
		tableProps:    config.Schema.TableProps,
		fieldDefs:     fieldDefs,
		preQueries:    config.PreQueries,
		postQueries:   config.PostQueries,
	}, nil
}

func (g *Generator) Generate() error {
	out, err := os.Create(g.generateFileName())
	if err != nil {
		return err
	}
	defer out.Close()

	if err = g.writeStrs(out, g.generatePreQueries()); err != nil {
		return err
	}

	if _, err = out.WriteString(g.generateCreateTable()); err != nil {
		return err
	}

	builder := strings.Builder{}
	builder.WriteString(fmt.Sprintf("BATCHINSERT INTO %s BATCHVALUES(", g.tableName))
	for i := int64(1); i <= g.numRows; i++ {
		batchValue := make([]string, len(g.fieldDefs))
		for j, fieldDef := range g.fieldDefs {
			var val string
			switch fieldDef.Type {
			case "int":
				var minInt, maxInt int64
				if fieldDef.Range != nil {
					minInt = int64(fieldDef.Range.Start.(int))
					maxInt = int64(fieldDef.Range.End.(int))
				} else {
					minInt, maxInt = -100, 100
				}
				val = fmt.Sprintf("%d", minInt+rand.Int63n(maxInt-minInt))
			case "float":
				var minFloat, maxFloat float64
				if fieldDef.Range != nil {
					minFloat = fieldDef.Range.Start.(float64)
					maxFloat = fieldDef.Range.End.(float64)
				} else {
					minFloat, maxFloat = -100, 100
				}
				val = fmt.Sprintf("%f", rand.Float64()*(maxFloat-minFloat)+minFloat)
			case "uint":
				var minUint, maxUint uint64
				if fieldDef.Range != nil {
					minUint = fieldDef.Range.Start.(uint64)
					maxUint = fieldDef.Range.End.(uint64)
				} else {
					minUint, maxUint = 0, 100
				}
				val = fmt.Sprintf("%d", uint64(rand.Int63n(int64(maxUint-minUint)))+minUint)
			case "string":
				var minLen, maxLen int
				if fieldDef.Range != nil {
					minLen = fieldDef.Range.Start.(int)
					maxLen = fieldDef.Range.End.(int)
				} else {
					minLen, maxLen = 0, 5
				}
				length := rand.Intn(maxLen-minLen+1) + minLen
				result := make([]byte, length)
				for i := range result {
					result[i] = charset[rand.Intn(len(charset))]
				}
				val = string(result)
			case "timestamp":
				var minTime, maxTime time.Time
				if fieldDef.Range != nil {
					minTime = fieldDef.Range.Start.(time.Time)
					maxTime = fieldDef.Range.End.(time.Time)
				} else {
					minTime = time.Date(2023, 1, 1, 0, 0, 0, 0, time.Local)
					maxTime = time.Date(2023, 12, 31, 23, 59, 59, 0, time.Local)
				}
				duration := maxTime.Sub(minTime)
				randomDuration := time.Duration(rand.Int63n(int64(duration)))
				val = minTime.Add(randomDuration).Format("2006-01-02 15:04:05.000")
			case "boolean":
				val = fmt.Sprintf("%t", rand.Int()%2 == 0)
			}
			batchValue[j] = val
		}
		builder.WriteString(fmt.Sprintf("VALUES (%s)", strings.Join(batchValue, ",")))

		if i%int64(g.batchNum) == 0 {
			builder.WriteString(");\n")
			if _, err = out.WriteString(builder.String()); err != nil {
				return err
			}
			builder.Reset()
			builder.WriteString(fmt.Sprintf("BATCHINSERT INTO %s BATCHVALUES(", g.tableName))
		} else {
			if i != g.numRows {
				builder.WriteRune(',')
			}
		}
	}
	if builder.Len() > 0 && builder.Len() != 30+len(g.tableName) {
		builder.WriteString(");\n")
		if _, err = out.WriteString(builder.String()); err != nil {
			return err
		}
	}

	if err = g.writeStrs(out, g.generatePostQueries()); err != nil {
		return err
	}

	return nil
}

func (g *Generator) generatePreQueries() []string {
	return g.preQueries
}

func (g *Generator) generatePostQueries() []string {
	return g.postQueries
}

func (g *Generator) generateCreateTable() string {
	builder := strings.Builder{}
	builder.WriteString(fmt.Sprintf("CREATE TABLE %s (", g.tableName))
	fields := make([]string, 0, len(g.fieldDefs))
	for _, fieldDef := range g.fieldDefs {
		fields = append(fields, fmt.Sprintf("%s %s", fieldDef.Name, fieldDef.Type))
	}
	builder.WriteString(strings.Join(fields, ","))
	builder.WriteString(fmt.Sprintf(") STORED AS %s", g.storageFormat))

	if len(g.tableProps) > 0 {
		builder.WriteString(" TBLPROPERTIES(")
		props := make([]string, 0, len(g.tableProps))
		for propK, propV := range g.tableProps {
			props = append(props, fmt.Sprintf("'%s'='%s'", propK, propV))
		}
		builder.WriteString(strings.Join(props, ","))
		builder.WriteString(")")
	}
	builder.WriteString(";\n")
	return builder.String()
}

func (g *Generator) generateFileName() string {
	return g.tableName + ".sql"
}

func (g *Generator) writeStrs(out *os.File, queries []string) error {
	for _, q := range queries {
		if _, err := out.WriteString(q + ";\n"); err != nil {
			return err
		}
	}
	return nil
}
