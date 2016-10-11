spark-bigquery
===============

[![Build Status](https://travis-ci.org/appsflyer-dev/spark-bigquery.png)](https://travis-ci.org/appsflyer-dev/spark-bigquery)

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

### Including spark-bigquery into your project

#### Maven

```xml
<repositories>
  <repository>
    <id>oss-sonatype</id>
    <name>oss-sonatype</name>
    <url>https://oss.sonatype.org/content/repositories/snapshots/</url>
    <snapshots>
      <enabled>true</enabled>
    </snapshots>
  </repository>
</repositories>

<dependencies>
  <dependency>
    <groupId>com.appsflyer</groupId>
    <artifactId>spark-bigquery_${scala.binary.version}</artifactId>
    <version>0.1.0-SNAPSHOT</version>
  </dependency>
</dependencies>
```

#### SBT

```sbt
resolvers += Opts.resolver.sonatypeSnapshots

libraryDependencies += "com.appsflyer" %% "spark-bigquery" % "0.1.0-SNAPSHOT"
```

### Saving DataFrame using BigQuery Hadoop writer API

```scala
import com.appsflyer.spark.bigquery._

val df = ...
df.saveAsBigQueryTable("project-id:dataset-id.table-name")
```

### Saving DataFrame using BigQuery streaming API

```scala
import com.appsflyer.spark.bigquery._

val df = ...
df.streamToBigQueryTable("project-id:dataset-id.table-name")
```

Notes on using this API:

 * Target data set must already exist

# License

Copyright 2016 Appsflyer.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
