package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"github.com/boltdb/bolt"
	"github.com/olekukonko/tablewriter"
)

var (
	db     *bolt.DB
	bucket string
)

type Record struct {
	key   string
	value *map[string]string
}

type Table struct {
	rows []Record
}

func (r *Table) renderTable() {

	sort.Sort(r)

	keys := []string{}
	data := [][]string{}

	for _, row := range r.rows {

		val := *row.value

		if len(keys) == 0 {
			keys = append(keys, "primary key")
			for k := range val {
				keys = append(keys, k)
			}
		}

		// for _, k := range keys {
		// 	if k == "primary key" {
		// 		log.Println(row.key)
		// 	} else {
		// 		log.Println(val[k])
		// 	}
		// }

		d := []string{}
		for _, k := range keys {
			if k == "primary key" {
				d = append(d, row.key)
			} else {
				d = append(d, val[k])
			}
		}
		data = append(data, d)
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(keys)
	table.AppendBulk(data)
	table.Render()
}

func (r *Table) Len() int           { return len(r.rows) }
func (r *Table) Swap(i, j int)      { r.rows[i], r.rows[j] = r.rows[j], r.rows[i] }
func (r *Table) Less(i, j int) bool { return r.rows[i].key < r.rows[j].key }

func main() {

	dbpath := flag.String("database", "", "Database path")
	flag.Parse()

	resv := make(chan bool)

	if _, err := os.Stat(*dbpath); os.IsNotExist(err) {
		fmt.Println(fmt.Sprintf("Database not found: %s", *dbpath))
		go func() { resv <- false }()
	}

	var err error

	db, err = bolt.Open(*dbpath, 0600, nil)
	if err != nil {
		fmt.Println(err.Error())
		go func() { resv <- false }()
	}
	defer db.Close()

	go func() {
		buf := bufio.NewReader(os.Stdin)
		for {
			fmt.Print("> ")
			if cmd, err := buf.ReadBytes('\n'); err != nil {
				fmt.Print(err.Error())
				resv <- false
			} else {
				c := strings.TrimSpace(strings.TrimRight(string(cmd), "\n"))
				if c == "" {
					continue
				}
				if c == "show buckets" {
					showBucketList()
				} else if matches := regexp.MustCompile(`^use=([a-zA-Z0-9]*)$`).FindStringSubmatch(c); matches != nil {
					bucket = matches[1]
				} else if matches := regexp.MustCompile(`^key=(.*)$`).FindStringSubmatch(c); matches != nil {
					if bucket == "" {
						fmt.Println("Selected bucket")
					} else {
						if err := findData(bucket, matches[1]); err != nil {
							fmt.Println(err.Error())
						}
					}
				} else if matches := regexp.MustCompile(`^bucket=([a-zA-Z0-9]*)\skey=(.*)`).FindStringSubmatch(c); matches != nil {
					if err := findData(matches[1], matches[2]); err != nil {
						fmt.Println(err.Error())
					}
					// } else if matches := regexp.MustCompile(`^bucket=([a-zA-Z0-9]*)$`).FindStringSubmatch(c); matches != nil {
					// 	if err := findData(matches[1], ""); err != nil {
					// 		fmt.Println(err.Error())
					// 	}
				} else if c == "quit" {
					resv <- false
				} else {
					fmt.Printf("Command not found: %s", string(cmd))
				}
			}
		}
	}()

	for s := range resv {
		if !s {
			fmt.Println("Exit...")
			close(resv)
		}
	}
}

func showBucketList() {
	db.View(func(tx *bolt.Tx) error {
		tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			fmt.Println(string(name))
			return nil
		})
		return nil
	})
}

func findData(backetName string, key string) error {
	var bucket *bolt.Bucket
	return db.View(func(tx *bolt.Tx) error {
		tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			if backetName == string(name) {
				bucket = b
			}
			return nil
		})

		if bucket == nil {
			return errors.New("bucket not found")
		}

		c := bucket.Cursor()
		prefix := []byte(key)
		data := map[string]interface{}{}

		for pk, v := c.Seek(prefix); bytes.HasPrefix(pk, prefix); pk, v = c.Next() {
			u := map[string]interface{}{}
			json.Unmarshal(v, &u)
			data[string(pk)] = u
		}

		showData(data)
		return nil
	})
}

func showData(d map[string]interface{}) {

	table := Table{rows: []Record{}}

	for pk, val := range d {

		value := map[string]string{}
		record := Record{key: pk, value: &value}
		table.rows = append(table.rows, record)

		if data, ok := val.(map[string]interface{}); ok {
			for k, v := range data {
				value[k] = fmt.Sprintf("%s", reflect.TypeOf(v))
				if iv, ok := v.([]interface{}); ok {
					// []interface{}
					data := map[string]interface{}{}
					for i, mv := range iv {
						data[fmt.Sprintf("%03d-%s|%s", i, pk, k)] = mv
					}
					defer showData(data)
				} else if mv, ok := v.(map[string]interface{}); ok {
					// map[string]interface{}
					data := map[string]interface{}{}
					data[fmt.Sprintf("%s|%s", pk, k)] = mv
					defer showData(data)
				} else if sv, ok := v.(string); ok {
					// string
					value[k] = sv
				}
			}
		}
	}

	table.renderTable()
}
