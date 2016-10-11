package com.appsflyer.spark.bigquery

import org.apache.spark.sql.streaming.DataStreamWriter

/**
  * Spark streaming support for BigQuery
  */
package object streaming {

  /**
    * Enhanced version of DataStreamWriter with BigQuery support.
    *
    * Sample usage:
    *
    * <code>df.writeStream.bigQueryTable("project-id:dataset-id.table-id")</code>
    */
  implicit class BigQueryDataFrameWriter(writer: DataStreamWriter[String]) extends Serializable {

    /**
      * Insert data into BigQuery table using streaming API
      *
      * @param fullyQualifiedOutputTableId output-table id of the form
      *                                    [optional projectId]:[datasetId].[tableId]
      * @param batchSize                   number of rows to write to BigQuery at once
      *                                    (default: 500)
      */
    def bigQueryTable(fullyQualifiedOutputTableId: String, batchSize: Int = 500): Unit = {
      val bigQueryWriter = new BigQueryStreamWriter(fullyQualifiedOutputTableId, batchSize)
      writer.foreach(bigQueryWriter)
        .start
    }
  }

}
