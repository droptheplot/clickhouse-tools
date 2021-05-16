package dump

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/droptheplot/clickhouse-tools/clickhouse_client"
)

type Row struct {
	name string
	engine string
	createTableQuery string
}

func Dump(orig, dest, user, password, url string, onCluster, dropTable bool, client *http.Client) {
	tables := clickhouse_client.Execute(
		client,
		fmt.Sprintf("SELECT database, name, engine, create_table_query FROM system.tables WHERE database = '%s';", orig),
		url,
		orig,
		user,
		password,
	)

	replacer := strings.NewReplacer("\\n", "\n", "\\'", "\u0027")

	rows := parseRows(tables)
	rows = sortRows(rows)

	if onCluster {
		addOnCluster(orig, dest, rows)
	}

	var schema []string

	for _, row := range rows {
		if dropTable {
			schema = append(schema, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s ON CLUSTER '{cluster}';", dest, row.name))
		}

		schema = append(schema, replacer.Replace(row.createTableQuery)+";\n")
	}

	fmt.Printf("%s", strings.Join(schema, "\n"))
}

func parseRows(tables string) []*Row {
	var rows []*Row

	for _, row := range strings.Split(tables, "\n") {
		columns := strings.Split(row, "\t")

		if len(columns) < 3 {
			continue
		}

		name := columns[1]
		engine := columns[2]
		createTableQuery := columns[3]

		if strings.HasPrefix(name, ".inner") || name == "" {
			continue
		}

		rows = append(rows, &Row{name, engine, createTableQuery})
	}

	return rows
}

func sortRows(rows []*Row) []*Row {
	for i, row := range rows {
		if row.engine != "View" && row.engine != "MaterializedView" {
			rows = append([]*Row{row}, append((rows)[:i], (rows)[i+1:]...)...)
		}
	}
	return rows
}

func addOnCluster(orig, dest string, rows []*Row) {
	for _, row := range rows {
		if strings.Contains(row.createTableQuery, "CLUSTER") {
			continue
		}

		var old string
		var new string

		switch row.engine {
		case "View":
			old = fmt.Sprintf("CREATE VIEW %s.%s", orig, row.name)
			new = fmt.Sprintf("CREATE VIEW %s.%s ON CLUSTER '{cluster}'", dest, row.name)
			break
		case "MaterializedView":
			old = fmt.Sprintf("CREATE MATERIALIZED VIEW %s.%s", orig, row.name)
			new = fmt.Sprintf("CREATE MATERIALIZED VIEW %s.%s ON CLUSTER '{cluster}'", dest, row.name)
			break
		default:
			old = fmt.Sprintf("CREATE TABLE %s.%s", orig, row.name)
			new = fmt.Sprintf("CREATE TABLE %s.%s ON CLUSTER '{cluster}'", dest, row.name)
			break
		}

		row.createTableQuery = strings.Replace(row.createTableQuery, old, new, 1)
	}
}
