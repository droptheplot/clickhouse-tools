package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"github.com/droptheplot/clickhouse-tools/dump"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

var defaultCert = "/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt"

func main() {
	if len(os.Args) <= 1 {
		log.Fatalf("Unknown action")
	}

	switch os.Args[1] {
	case "dump":
		flagSet := flag.NewFlagSet("dump", flag.ContinueOnError)

		var orig = flagSet.String("orig", "", "Original database name")
		var dest = flagSet.String("dest", "", "Destination database name")
		var user = flagSet.String("user", "", "Clickhouse user")
		var password = flagSet.String("password", "", "Clickhouse password")
		var url = flagSet.String("url", "", "Clickhouse URL")

		var cert = flagSet.String("cert", "", "Clickhouse certificate")
		var onCluster = flagSet.Bool("on-cluster", false, "Add ON CLUSTER to existing tables")
		var dropTable = flagSet.Bool("drop-table", false, "Add DROP TABLE to each table")

		if err := flagSet.Parse(os.Args[2:]); err != nil {
			flagSet.PrintDefaults()
		}

		if dest == nil {
			dest = orig
		}

		dump.Dump(*orig, *dest, *user, *password, *url, *onCluster, *dropTable, defaultHTTPClient(*cert))

		break
	default:
		log.Fatalf("Unknown action: %s", os.Args[1])
	}
}

func defaultHTTPClient(cert string) *http.Client {
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

