# csv2sqlite

This is a simple tool to import csv files into sqlite. It offers a few
niceties over

```
.headers on
.mode csv
.import foo.csv foo
```

- it normalizes column names: removing symbols and downcasing
- it can import multiple csvs properly (it won't add the header as a row)
- it can smartly add columns to the table if there are new columns
  present in subsequent files
