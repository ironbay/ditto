package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"gopkg.in/alecthomas/kingpin.v1"
)

var (
	csv         = kingpin.Flag("csv", "Output to CSV (required for counter tables)").Bool()
	destination = kingpin.Flag("destination", "Destination server").String()
	table       = kingpin.Flag("table", "Destination table to export data into").String()

	source = kingpin.Arg("source", "Source server").Required().String()
	query  = kingpin.Arg("query", "CQL to select data to export").Required().String()
)

func random(min, max int) int {
	rand.Seed(time.Now().Unix())
	return rand.Intn(max-min) + min
}

func main() {
	kingpin.Parse()

	sc := gocql.NewCluster(*source)
	source, err := sc.CreateSession()
	source.SetPageSize(10000)
	if err != nil {
		return
	}
	defer source.Close()

	var worker Worker
	worker.Count = 200
	worker.Produce = func(queue chan []interface{}) {
		i := source.Query(*query).Iter()
		data, _ := i.RowData()
		for i.Scan(data.Values...) {
			queue <- []interface{}{data.Values, i.Columns()}
			data, _ = i.RowData()
		}
	}

	if !*csv {
		dc := gocql.NewCluster(*destination)
		destination, err := dc.CreateSession()
		if err != nil {
			return
		}
		defer destination.Close()
		worker.Consume = func(args []interface{}) bool {
			values := args[0].([]interface{})
			columns := args[1].([]gocql.ColumnInfo)
			empty := make([]string, len(values))
			keys := make([]string, len(values))
			for i, _ := range columns {
				empty[i] = "?"
				keys[i] = columns[i].Name
				i += 1
			}
			query := "INSERT INTO " + *table + " (" + strings.Join(keys, ",") + ") VALUES (" + strings.Join(empty, ",") + ")"
			q := destination.Query(query, values...)
			err := q.Exec()
			if err != nil {
				log.Println(err)
				return true
			}
			return false
		}
	} else {
		f, err := os.Create("temp.csv")
		if err != nil {
			log.Println(err)
			return
		}

		defer f.Close()
		worker.Consume = func(args []interface{}) bool {
			data := args[0].([]interface{})
			columns := args[1].([]gocql.ColumnInfo)
			values := make([]string, len(data))
			for i, k := range columns {
				v := data[i]
				s := fmt.Sprint(v)
				if k.TypeInfo.Type() == gocql.TypeSet {
					s = "{'" + strings.Join(v.([]string), "', '") + "'}"
				}
				if k.TypeInfo.Type() == gocql.TypeList {
					s = "['" + strings.Join(v.([]string), "', '") + "']"
				}
				values[i] = s
				i += 1
			}
			f.WriteString("\"" + strings.Join(values, "\",\"") + "\"\n")
			return false
		}
	}
	worker.Run()
}
