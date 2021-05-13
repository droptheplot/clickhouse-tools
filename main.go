package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
)

type Row struct {
	name string
	engine string
	createTableQuery string
}

var defaultCert = "/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt"

func main() {
	var orig = flag.String("orig", "", "Original database name")
	var dest = flag.String("dest", "", "Destination database name")
	var user = flag.String("user", "", "Clickhouse user")
	var password = flag.String("password", "", "Clickhouse password")
	var url = flag.String("url", "", "Clickhouse URL")

	var cert = flag.String("cert", "", "Clickhouse certificate")
	var onCluster = flag.Bool("on-cluster", false, "Add ON CLUSTER to existing tables")
	var dropTable = flag.Bool("drop-table", false, "Add DROP TABLE to each table")

	flag.Parse()

	if dest == nil {
		dest = orig
	}

	if orig == nil || user == nil || password == nil || url == nil {
		flag.PrintDefaults()
		os.Exit(1)
	}

	client := getClient(*cert)

	tables := executeQuery(
		client,
		fmt.Sprintf("SELECT database, name, engine, create_table_query FROM system.tables WHERE database = '%s';", *orig),
		*url,
		*orig,
		*user,
		*password,
	)

	replacer := strings.NewReplacer("\\n", "\n", "\\'", "\u0027")

	rows := parseRows(tables)
	rows = sortRows(rows)

	if *onCluster {
		addOnCluster(*orig, *dest, rows)
	}

	var schema []string

	for _, row := range rows {
		if *dropTable {
			schema = append(schema, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s ON CLUSTER '{cluster}';", *dest, row.name))
		}

		schema = append(schema, replacer.Replace(row.createTableQuery)+";\n")
	}

	fmt.Printf("%s", strings.Join(schema, "\n"))
}

func getClient(cert string) *http.Client {
	if cert == "" {
		cert = defaultCert
	}

	caCert, err := ioutil.ReadFile(cert)

	if err != nil {
		log.Fatal(err)
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs: caCertPool,
			},
		},
	}

	return client
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
	// Move tables to the top
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

func executeQuery(client *http.Client, sql, url, database, user, password string) string {
	req, _ := http.NewRequest("GET", url, nil)
	query := req.URL.Query()
	query.Add("database", database)
	query.Add("query", sql)

	req.URL.RawQuery = query.Encode()

	req.Header.Add("X-ClickHouse-User", user)
	req.Header.Add("X-ClickHouse-Key", password)

	resp, err := client.Do(req)

	if err != nil {
		log.Fatal(err)
	}

	data, _ := ioutil.ReadAll(resp.Body)

	return string(data)
}
