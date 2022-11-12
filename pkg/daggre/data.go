package daggre

import (
	"fmt"
	"github.com/utmhikari/daggre/pkg/util"
	"log"
)

// Row is the record of data
type Row map[string]interface{}

// Copy get copy of current row data
func (r *Row) Copy() *Row {
	cp := &Row{}
	err := util.DeepCopyByJson(r, cp)
	if err != nil {
		// TODO: handle error
		log.Panicf("failed to copy row, %s\n", err.Error())
		return nil
	}
	return cp
}

// Equals returns true if the row is equal to the other
func (r *Row) Equals(other *Row) bool {
	return fmt.Sprint(*r) == fmt.Sprint(*other)
}

// Table is the set of rows
type Table []*Row

// AppendRow appends a copied row to current table
func (t *Table) AppendRow(r *Row) {
	if r != nil {
		rowCopy := r.Copy()
		if rowCopy != nil {
			*t = append(*t, rowCopy)
		}
	}
}

// ToString returns a pretty formatted string representation of the table
func (t *Table) ToString() string {
	s := util.JsonDump(*t)
	if len(s) == 0 {
		return "<INVALID DAGGRE TABLE>"
	}
	return s
}

// Equals returns true if the table is equal to the other
func (t *Table) Equals(other *Table) bool {
	if len(*t) != len(*other) {
		return false
	}
	for i := 0; i < len(*t); i++ {
		row1, row2 := (*t)[i], (*other)[i]
		if !row1.Equals(row2) {
			log.Printf("[%d] -> %+v != %+v\n", i, *row1, *row2)
			return false
		}
	}
	return true
}

// Data is the collection of tables
type Data map[string]*Table

// GetTable get a copy of specific table
func (d *Data) GetTable(name string) *Table {
	tb, ok := (*d)[name]
	if !ok {
		return nil
	}

	tbCopy := Table{}
	for _, row := range *tb {
		tbCopy.AppendRow(row)
	}
	return &tbCopy
}

// GetMergedTables get copies of specific tables merged into one single table
func (d *Data) GetMergedTables(names ...string) *Table {
	tbAll := Table{}
	for _, name := range names {
		tb := d.GetTable(name)
		if tb != nil {
			for _, row := range *tb {
				tbAll.AppendRow(row)
			}
		}
	}
	return &tbAll
}

// ToString returns a pretty formatted string representation of the table
func (d *Data) ToString() string {
	s := util.JsonDump(*d)
	if len(s) == 0 {
		return "<INVALID DAGGRE DATA>"
	}
	return s
}
