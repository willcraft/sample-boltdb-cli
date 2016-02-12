package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/boltdb/bolt"
	"github.com/olekukonko/tablewriter"
)

var db *bolt.DB

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
				if c == "show buckets" {
					showBucketList()
				} else if matches := regexp.MustCompile(`^bucket=([a-zA-Z0-9]*)\skey=(.*)`).FindStringSubmatch(c); matches != nil {
					if err := showData(matches[1], matches[2]); err != nil {
						fmt.Println(err.Error())
					}
				} else if matches := regexp.MustCompile(`^bucket=([a-zA-Z0-9]*)$`).FindStringSubmatch(c); matches != nil {
					if err := showData(matches[1], ""); err != nil {
						fmt.Println(err.Error())
					}
				} else {
					fmt.Printf("Command not found: %s", string(cmd))
				}
			}
		}
	}()

	for s := range resv {
		if !s {
			fmt.Println("Quit...")
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

func showData(backetName string, key string) error {
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

		keys := []string{}
		data := [][]string{}
		c := bucket.Cursor()
		prefix := []byte(key)

		for pk, v := c.Seek(prefix); bytes.HasPrefix(pk, prefix); pk, v = c.Next() {

			u := map[string]string{}
			json.Unmarshal(v, &u)

			if len(keys) == 0 {
				keys = append(keys, "primary key")
				for k := range u {
					keys = append(keys, k)
				}
			}

			d := []string{}
			for _, k := range keys {
				if k == "primary key" {
					d = append(d, string(pk))
				} else {
					d = append(d, u[k])
				}
			}

			data = append(data, d)
		}

		if len(data) == 0 {
			fmt.Println()
		} else {
			renderTable(keys, data)
		}

		return nil
	})
}

func renderTable(header []string, data [][]string) {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(header)
	table.AppendBulk(data)
	table.Render()
}
