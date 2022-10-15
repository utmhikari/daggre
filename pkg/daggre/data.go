package daggre

import (
	"encoding/json"
	"fmt"
	"github.com/utmhikari/daggre/pkg/util"
	"log"
)

type Row map[string]interface{}

func (r *Row) Copy() *Row {
	cp := &Row{}
	err := util.DeepCopyByJson(r, cp)
	if err != nil {
		log.Panicf("failed to copy row, %s\n", err.Error())
		return nil
	}
	return cp
}

func (r *Row) Equals(other *Row) bool {
	return fmt.Sprint(*r) == fmt.Sprint(*other)
}

type Table []*Row

func (t *Table) AppendRow(r *Row) {
	if r != nil {
		rowCopy := r.Copy()
		if rowCopy != nil {
			*t = append(*t, rowCopy)
		}
	}
}

func (t *Table) ToString() string {
	jsonBytes, err := json.MarshalIndent(*t, "", "  ")
	if err != nil {
		return "<INVALID TABLE>"
	}
	return string(jsonBytes)
}

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
