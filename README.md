spark-bigquery
===============

This Spark module allows saving DataFrame as BigQuery table.

## Usage

```scala
import com.appsflyer.spark.bigquery.BigQuerySpark._

val sqlContext = ...
sqlContext.setBigQueryGcsBucket("temp-bucket")
sqlContext.setBigQueryProjectId("project-id")

val df = ...
df.saveAsBigQueryTable("project-id:dataset-id.table-name")
```

# License

Copyright 2016 Appsflyer.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
