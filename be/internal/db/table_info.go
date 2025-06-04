package db

import (
	om "github.com/elliotchance/orderedmap/v3"
	"iter"
)

type TableInfoV2 struct {
	Name string
	// Columns is a map of alias to ColumnInfo
	Columns *om.OrderedMap[string, *ColumnInfo]
	Alias   string // To be reviewed
	HasStar bool   // To be reviewed
	// UnAuthorizedTables is a list of tables that are unauthorized inside this table, note that name of the table should be real name in the database
	// UnAuthorizedTables is a map of alias to array of TableInfo
	// UnAuthorizedTables is a map of alias to TableInfo
	UnAuthorizedTables *om.OrderedMap[string, *TableInfoV2] // Consider use ordered map if needed
	Authorized         bool
	IsDatabase         bool
}

func (t *TableInfoV2) createColumnFromSourceTable(colName string, sourceTable *TableInfoV2, newColAlias *string) {
	if t.Columns == nil {
		t.Columns = om.NewOrderedMap[string, *ColumnInfo]()
	}
	if _, ok := sourceTable.Columns.Get(colName); ok {
		colAlias := colName
		if newColAlias != nil {
			colAlias = *newColAlias
		}

		t.Columns.Set(colAlias, &ColumnInfo{
			Name:        colName,
			SourceTable: sourceTable,
		})
	}
}

func (t *TableInfoV2) createAllColumnFromSourceTable(sourceTable *TableInfoV2, newColAliases []string) {
	if sourceTable == nil || sourceTable.Columns == nil {
		return
	}
	curIndex := 0
	for colAlias, _ := range sourceTable.Columns.AllFromFront() {
		var newColAlias *string
		if newColAliases != nil && curIndex < len(newColAliases) {
			newColAlias = &(newColAliases)[curIndex]
			curIndex++
		}
		t.createColumnFromSourceTable(colAlias, sourceTable, newColAlias)
	}
}

func (t *TableInfoV2) createAllColumnsFromSourceTableList(sourceTable iter.Seq[*TableInfoV2], newColAliases []string) {
	curIndex := 0
	for table := range sourceTable {
		if table == nil || table.Columns == nil {
			continue // Skip nil tables, but this should not happen
		}

		// Check if newColAliases is nil or if there are enough aliases left
		if newColAliases != nil && curIndex < len(newColAliases) {
			endIndex := curIndex + table.Columns.Len()
			// Special cases, we can have lesser aliases than columns
			if endIndex > len(newColAliases) {
				endIndex = len(newColAliases)
			}

			// Dereference the pointer and then slice
			t.createAllColumnFromSourceTable(table, newColAliases[curIndex:endIndex])

			// Update the index for the next table
			curIndex = endIndex
		} else {
			// No aliases available, pass nil
			t.createAllColumnFromSourceTable(table, nil)
		}
	}
}

func (t *TableInfoV2) getAliasOfColumn(str string) string {
	for alias, col := range t.Columns.AllFromFront() {
		if col.SourceTable.IsDatabase {
			if col.Name == str {
				return alias // or return col.Alias
			}
		} else {
			// column come from a subquery
			recurResult := col.SourceTable.getAliasOfColumn(str)
			if recurResult != "" {
				return alias
			}
		}
	}
	return ""
}

// Clone creates a deep copy of the TableInfoV2 instance.
// But we will ignore the SourceTable field in ColumnInfo and the UnAuthorizedTables field in TableInfoV2.
// Because these cause stack overflow when cloning.
func (t *TableInfoV2) Clone() *TableInfoV2 {
	if t == nil {
		return nil
	}

	clone := &TableInfoV2{
		Name:       t.Name,
		Alias:      t.Alias,
		HasStar:    t.HasStar,
		IsDatabase: t.IsDatabase,
		Authorized: t.Authorized,
	}

	// Clone Columns
	if t.Columns != nil {
		clone.Columns = om.NewOrderedMap[string, *ColumnInfo]()
		for alias, col := range t.Columns.AllFromFront() {
			if clone.IsDatabase {
				clone.Columns.Set(alias, col.Clone(clone))
			} else {
				clone.Columns.Set(alias, col.Clone(nil))
			}
		}
	}

	// Clone UnAuthorizedTables
	if t.UnAuthorizedTables != nil {
		clone.UnAuthorizedTables = om.NewOrderedMap[string, *TableInfoV2]()
		for alias, unAuthTable := range t.UnAuthorizedTables.AllFromFront() {
			clone.UnAuthorizedTables.Set(alias, unAuthTable.Clone())
		}
	}

	return clone
}

// getRealColName returns the real column name for a given column alias.
// If current table is virtual, it will resolve the column name by traversing the source tables.
func (t *TableInfoV2) getRealColName(colAlias string) string {
	resolvedCol := t.getRealColumnInfoFromAlias(colAlias)
	if resolvedCol == nil {
		return ""
	}
	return resolvedCol.Name
}

func (t *TableInfoV2) getRealColumnInfoFromAlias(colAlias string) *ColumnInfo {
	curTable := t
	// Find the real table
	for curTable != nil && !curTable.IsDatabase {
		resolvedCol, ok := curTable.Columns.Get(colAlias)
		if !ok {
			return nil
		}
		colAlias = resolvedCol.Name
		curTable = resolvedCol.SourceTable
	}

	if curTable == nil {
		return nil
	}
	resolvedCol, ok := curTable.Columns.Get(colAlias)
	if !ok {
		return nil
	}

	return resolvedCol
}

type ColumnInfo struct {
	Name        string
	SourceTable *TableInfoV2
}

// Clone Create a copy of the ColumnInfo instance.
// If sourceTable is nil, it will also clone the source table
func (c *ColumnInfo) Clone(sourceTable *TableInfoV2) *ColumnInfo {
	if c == nil {
		return nil
	}

	clone := &ColumnInfo{
		Name: c.Name,
	}

	if sourceTable != nil {
		clone.SourceTable = sourceTable
	} else {
		clone.SourceTable = c.SourceTable.Clone()
	}

	return clone
}
