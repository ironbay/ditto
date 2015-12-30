#Ditto

Ditto streams the result of any Cassandra query to be into another table. Useful for transfering data between clusters, migrating changes to schema, or batch processing. Executes writes concurrently so it has speed advantages over COPY TO as well as avoiding the unecessary csv generation


`$ ditto <source> <destination> <table> <query>`
```
Flags:
  --help  Show help.

Args:
  <source>       Source server
  <destination>  Destination server
  <table>        Destination table to import data into
  <query>        CQL to select data to export
```

## Examples

####Copy data from old schema to new table:
`$ ditto localhost localhost test.new_table "SELECT * FROM test.old_table"`

####Import days worth of data from production cluster to debug cluster:
`$ ditto production localhost test.data "SELECT * FROM test.data WHERE date = '2015-01-02'"`
