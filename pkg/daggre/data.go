package daggre

import (
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

type Table []Row

type Data map[string]*Table

// GetTable get a copy of specific table
func (d *Data) GetTable(name string) *Table {
	tb, ok := (*d)[name]
	if !ok {
		return nil
	}

	tbCopy := Table{}
	for _, row := range *tb {
		rowCopy := row.Copy()
		if rowCopy != nil {
			tbCopy = append(tbCopy, *rowCopy)
		}
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
				rowCopy := row.Copy()
				if rowCopy != nil {
					tbAll = append(tbAll, *rowCopy)
				}
			}
		}
	}
	return &tbAll
}
