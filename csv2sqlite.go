package main

import (
	"bytes"
	"compress/gzip"
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"regexp"
	"strings"

	_ "modernc.org/sqlite"
)

var (
	db            = flag.String("db", "csv.db", "Database file")
	tableName     = flag.String("table", "csv", "Table name")
	createColumns = flag.Bool("create-columns", true, "Create any missing columns in table")
	trunc         = flag.Bool("trunc", false, "Truncate table before inserting")
	ephemeral     = flag.Bool("i", false, "Create an ephemeral db and start an interactive session")
	separatorStr  = flag.String("separator", ",", "Record separator")
	headerF       = flag.String("header", "", "Comma seperated header to use (files will be assumed to have no header")
	separator     rune
)

func main() {
	flag.Parse()

	if len(*separatorStr) != 1 {
		log.Fatalf("--separator must be a single character")
	}

	s := *separatorStr
	separator = rune(s[0])

	args := flag.Args()
	if len(args) < 1 {
		log.Fatalf("usage: %s <input.csv> [input2.csv...]", os.Args[0])
	}

	if *trunc {
		truncateTable()
	}

	if *ephemeral {
		f, err := ioutil.TempFile("", "csv2sqlite")
		if err != nil {
			log.Fatalf("create tmpfile err: %s", err)
		}
		f.Close()
		name := f.Name()
		defer os.Remove(name)
		*db = name
	}

	for _, filename := range args {
		processCSV(filename)
	}

	if *ephemeral {
		cmd := exec.Command("sqlite3", *db)
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		err := cmd.Run()
		if err != nil {
			log.Fatal(err)
		}
	}
}

func truncateTable() {
	db, err := sql.Open("sqlite", *db)
	if err != nil {
		log.Fatalf("open db err: %s", err)
	}
	defer db.Close()

	_, err = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", *tableName))
	if err != nil {
		log.Fatalf("Drop table err: %s", err)
	}
}

func decompressReader(f *os.File) (io.Reader, error) {
	if strings.HasSuffix(f.Name(), ".gz") {
		return gzip.NewReader(f)
	}

	return f, nil
}

func processCSV(filename string) {
	f, err := os.Open(filename)
	if err != nil {
		log.Fatalf("open file err: %s", err)
	}
	defer f.Close()

	dr, err := decompressReader(f)
	if err != nil {
		log.Fatalf("decompressReader err: %s", err)
	}

	db, err := sql.Open("sqlite", *db)
	if err != nil {
		log.Fatalf("open db err: %s", err)
	}
	defer db.Close()

	_, err = db.Exec("PRAGMA journal_mode = WAL")
	if err != nil {
		log.Fatalf("PRAGMA journal_mode = WAL err: %s", err)
	}

	r := csv.NewReader(dr)
	r.Comma = separator
	var header []string
	if *headerF != "" {
		headerReader := csv.NewReader(bytes.NewBufferString(*headerF))
		header, err = headerReader.Read()
		if err != nil {
			log.Fatalf("-header parse err: %s", err)
		}
	} else {
		header, err = r.Read()
		if err != nil {
			log.Fatalf("csv read header err: %s", err)
		}
	}

	missingHeaders := make(map[string]struct{})
	regex := regexp.MustCompile(`[^a-zA-Z0-9]+`)
	for i, h := range header {
		h = strings.ToLower(h)
		h = strings.TrimSpace(h)
		h = regex.ReplaceAllString(h, "_")
		header[i] = h
		missingHeaders[h] = struct{}{}
	}

	createStmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n\t%s)", *tableName, strings.Join(header, ",\n\t"))
	_, err = db.Exec(createStmt)
	if err != nil {
		log.Fatalf("Create table err: %s (%s)", err, createStmt)
	}

	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", *tableName))
	if err != nil {
		log.Fatalf("query table_info err: %s", err)
	}

	for rows.Next() {
		var (
			cid        interface{}
			name       string
			colType    string
			notnull    interface{}
			dflt_value interface{}
			pk         interface{}
		)

		err = rows.Scan(&cid, &name, &colType, &notnull, &dflt_value, &pk)
		if err != nil {
			log.Fatalf("Scan table_info err: %s", err)
		}

		delete(missingHeaders, name)
	}
	err = rows.Close()
	if err != nil {
		log.Fatalf("query table_info err: %s", err)
	}

	for k := range missingHeaders {
		_, err = db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", *tableName, k))
		if err != nil {
			log.Fatalf("Add column %s err: %s", k, err)
		}
	}

	qs := strings.Repeat("?,", len(header))
	qs = qs[:len(qs)-1]

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := tx.Prepare(fmt.Sprintf("INSERT INTO %s (%s) values (%s)", *tableName, strings.Join(header, ","), qs))
	if err != nil {
		log.Fatalf("Prepare insert err: %s", err)
	}

	rowFace := make([]interface{}, len(header))
	for {
		line, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatalf("Read err %s: %s", filename, err)
		}

		for i, v := range line {
			rowFace[i] = v
		}

		_, err = stmt.Exec(rowFace...)
		if err != nil {
			log.Fatal(err)
		}
	}

	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}
}
