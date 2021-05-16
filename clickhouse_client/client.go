package clickhouse_client

import (
	"io/ioutil"
	"log"
	"net/http"
)

func Execute(client *http.Client, sql, url, database, user, password string) string {
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
