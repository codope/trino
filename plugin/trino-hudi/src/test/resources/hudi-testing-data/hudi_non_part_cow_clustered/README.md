# Context

Hudi can be configured in a way that it optimizes the data layout on the storage it uses in order to improve
performance of read and write operations. This feature is called [clustering](https://hudi.apache.org/docs/next/clustering/).
During clustering, special types of commits (
[replace commits](https://hudi.apache.org/tech-specs/#transaction-log-timeline)) are created. These commits need to be
parsed differently when handing the Hudi timeline in the Trino Hudi plugin. In order to test that logic, we need
to have a Hudi table with replace commits.

# Process

In order to create such table, the following procedure is proposed. In the case of the following Spark/Hudi configuration
parameters are set:

```
hoodie.clustering.inline=true.
hoodie.clustering.inline.max.commits=1
```

Then clustering will be performed during every write operation and at least one replace commit will be created.
Practically any Hudi table (even a very simple one) will have replace commits if the above configuration parameters are
set when it is saved. The presence of a single replace commit will be enough for the Trino Hudi plugin to enter the 
code part where these commits are handled.

In order to keep things simple, one can just take the already existing `hudi_non_part_cow` Copy-On-Write table, read it from
a locally running test Spark job, and write it out with the parameters above set. The rest of the parameters were
coming from the metadata of the existing table, with the purpose of to make a table very similar to the already existing
(and working) one.

For example, given a blank Spark SQL shell with Hudi support, the following Spark SQL can be used to create such test table 
(after replacing the appropriate locations for the existing table and the new table):

```sql
CREATE TABLE hudi_non_part_cow
USING hudi
OPTIONS (
hoodie.table.name = 'hudi_non_part_cow',
hoodie.datasource.write.recordkey.field = 'rowId',
hoodie.datasource.write.precombine.field = 'preComb',
hoodie.table.type = 'COPY_ON_WRITE'
)
LOCATION 'file:///<<< EXISTING TABLE LOCATION >>>'
CREATE TABLE hudi_non_part_cow_clustered
USING hudi
OPTIONS (
   hoodie.table.name = 'hudi_non_part_cow_clustered',
   hoodie.datasource.write.recordkey.field = 'rowId',
   hoodie.datasource.write.precombine.field = 'preComb',
   hoodie.table.type = 'COPY_ON_WRITE',
   hoodie.clustering.inline = 'true',
   hoodie.clustering.inline.max.commits = '1'
)
LOCATION 'file:///<<< NEW TABLE LOCATION >>>'
AS SELECT * FROM hudi_non_part_cow
```
Note the two clustering parameters at the end of the second `CREATE TABLE` statement. Those are responsible for
creating a single `replacecommit` in the output.
