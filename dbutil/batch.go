package dbutil

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/lib/pq"
)

// MaxInsertionParameterNumber ...
var MaxInsertionParameterNumber = 65535

/*
Record represents a database record whose columns are reflected by `db` tag, e.g.
	type Person struct {
		ID     int 	  `db:"id,primary"`
		Name   string `db:"name"`
		IsMale bool	  `db:"is_male"`
	}
*/
type Record interface{}

// Batch ...
type Batch struct {
	table            string
	records          []Record
	columns          []string
	columnToField    map[string]string
	onConflictClause string
}

// NewBatch ...
func NewBatch(table string) *Batch {
	var b Batch
	b.table = table
	return &b
}

// MustAdd directly add a record without any sanity check. It is the caller's duty to ensure record homogeneity.
func (b *Batch) MustAdd(r Record) {
	b.records = append(b.records, r)

	if b.columnToField != nil {
		return
	}

	// make db column to field name map
	b.makeColumnMap(r)
}

// SetOnConflict ...
func (b *Batch) SetOnConflict(clause string) {
	b.onConflictClause = clause
}

// Insert ...
func (b *Batch) Insert(dbase *sql.DB) (numInserted int, err error) {
	callback := func(sql string, args ...interface{}) error {
		res, e := dbase.Exec(sql, args...)
		if e != nil {
			return e
		}

		num, e := res.RowsAffected()
		if e != nil {
			return e
		}

		numInserted += int(num)
		return nil
	}

	err = b.iterInsertQuery("", callback)

	return
}

// InsertIntPKs ...
func (b *Batch) InsertIntPKs(dbase *sql.DB, pkName string) (pks []int, err error) {
	callback := func(sql string, args ...interface{}) error {
		rows, e := dbase.Query(sql, args...)
		if e != nil {
			return e
		}

		for rows.Next() {
			var pk int
			if e := rows.Scan(&pk); e != nil {
				return e
			}
			pks = append(pks, pk)
		}

		return nil
	}

	err = b.iterInsertQuery(fmt.Sprintf("RETURNING %s", pkName), callback)

	return
}

type execSQLFunc func(sql string, args ...interface{}) error

func (b *Batch) iterInsertQuery(returnClause string, execFunc execSQLFunc) (err error) {
	if len(b.records) == 0 {
		return
	}

	numColumns := len(b.columns)
	batchSize := MaxInsertionParameterNumber / numColumns
	numBatch := (len(b.records)-1)/batchSize + 1
	for i := 0; i < numBatch; i++ {
		end := (i + 1) * batchSize
		if end >= len(b.records) {
			end = len(b.records)
		}

		sql, args, e := b.makeInsertQuery(i*batchSize, end, returnClause)
		if e != nil {
			err = e
			return
		}

		err = execFunc(sql, args...)
		if err != nil {
			return
		}
	}

	return
}

func (b *Batch) makeColumnMap(r Record) {
	b.columnToField = make(map[string]string)

	val := reflect.ValueOf(r)
	for i := 0; i < val.Type().NumField(); i++ {
		field := val.Type().Field(i)

		column := field.Tag.Get("db")
		if column == "" {
			continue
		}

		b.columnToField[column] = field.Name
	}

	for col := range b.columnToField {
		b.columns = append(b.columns, col)
	}
}

func (b *Batch) makeInsertQuery(start int, end int, returnClause string) (sql string, args []interface{}, err error) {
	columns := b.columns

	var phds []string
	for i := 0; i < end-start; i++ {
		o := len(columns) * i

		var ps []string
		for j := 0; j < len(columns); j++ {
			ps = append(ps, fmt.Sprintf("$%d", o+j+1))
		}

		phd := strings.Join(ps, ",")
		phd = fmt.Sprintf("(%s)", phd)
		phds = append(phds, phd)
	}

	sql = fmt.Sprintf(`INSERT INTO %s (%s) VALUES %s %s %s`,
		b.table,
		strings.Join(columns, ","),
		strings.Join(phds, ","),
		b.onConflictClause,
		returnClause,
	)

	for _, r := range b.records[start:end] {
		args = append(args, b.getValues(r)...)
	}

	return
}

func (b *Batch) getValues(r interface{}) (vs []interface{}) {
	val := reflect.ValueOf(r)
	for _, col := range b.columns {
		field := b.columnToField[col]
		v := val.FieldByName(field).Interface()

		rt := reflect.TypeOf(v)
		if rt.Kind() == reflect.Slice {
			v = pq.Array(v)
		}

		vs = append(vs, v)
	}
	return vs
}
