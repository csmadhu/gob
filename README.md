# gob
Bulk upserts to PostgreSQL, Cassandra and Redis

# Goal
* API to upsert large number of records to PostgreSQL, Cassandra and Redis

# Prerequistes
* GO 1.15 and above

# How it works
* Data models are defined using structs.
* Attributes should be mapped to storage layer using tag `db`
* Data model should have `Primarykey` attribute mapped to storage layer to be updated. 
* Records are inserted to database in batches. Size of the batch is configurable. Default batch size is 10000.

