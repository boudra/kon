# Kon

Kon is a data conversion command line tool and library.

**Project in active development**

# Guide

```bash
$ kon [input_options] [input_file] [output_options] [output_file]
```

Kon will guess the file type based on the filename extension if provided


```bash
$ kon example.csv example.json
```

You can use `--input/-i` and `--output/-o` to specify the types:

```bash
$ kon -i csv example.csv -o json example.json
$ cat example.csv | kon -i csv -o json > example.json
```

Examples:

```bash
$ kon -i ndjson example.json --csv-delimiter=\t example.csv
$ kon --csv-delimiter=, example.csv --csv-delimiter=\t example.tsv
$ kon --json-flatten example.json --sql-syntax=postgres --sql-create-table database.sql 
$ kon example.xlsx example.csv
```
