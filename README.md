spark-bigquery
===============

This Spark module allows saving DataFrame as BigQuery table.

The project was inspired by [spotify/spark-bigquery](https://github.com/spotify/spark-bigquery), but there are several differences:

* JSON is used as an intermediate format instead of Avro. This allows having fields on different levels named the same:

```json
{
  "obj": {
    "data": {
      "data": {}
    }
  }
}
```
* DataFrame's schema is automatically adapted to a legal one:

  1. Illegal characters are replaced with `_`
  2. Field names are converted to lower case to avoid ambiguity
  3. Duplicate field names are given a numeric suffix (`_1`, `_2`, etc.)

## Usage

### Saving DataFrame as BigQuery table

```scala
import com.appsflyer.spark.bigquery.BigQuerySpark._

val df = ...
df.saveAsBigQueryTable("project-id:dataset-id.table-name")
```

Please note that data set must already exist.

# License

Copyright 2016 Appsflyer.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
