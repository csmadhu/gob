# gob
Bulk Inserts to Postgres, Cassandra and Redis

# Goal
* API to insert large number of records to Postgres, Cassandra and Redis
* API to update large number of records to Postgres, Cassandra and Redis

# Prerequistes
* GO 1.15 and above

# How it works
* Data models are defined using structs.
* Attributes should be mapped to storage layer using tag `db`
* Data model should have Primarykey attribute to be updated. 
* Records are inserted to database in batches. Size of the batch is configurable. Default batch size is 10000.

