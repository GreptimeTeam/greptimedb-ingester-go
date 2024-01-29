package row

import "github.com/GreptimeTeam/greptimedb-ingester-go/table/schema/cell"

type Row struct {
	Values []cell.Cell
}

func New(columnCount uint) Row {
	return Row{Values: make([]cell.Cell, columnCount)}
}

func (r *Row) AddCell(cells ...cell.Cell) {
	// TODO
}
