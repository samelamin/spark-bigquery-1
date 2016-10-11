package com.appsflyer.spark

import com.appsflyer.spark.bigquery.streaming._
import com.google.cloud.hadoop.io.bigquery.{BigQueryOutputFormat, BigQueryConfiguration}
import com.google.gson.JsonParser
import org.apache.spark.sql._

package object bigquery {

  /**
    * Enhanced version of SQLContext with BigQuery support.
    */
  implicit class BigQuerySQLContext(sqlContext: SQLContext) extends Serializable {

    @transient
    lazy val hadoopConf = sqlContext.sparkContext.hadoopConfiguration

    /**
      * Set GCP project ID for BigQuery.
      */
    def setBigQueryProjectId(projectId: String): Unit = {
      hadoopConf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId)
    }

    /**
      * Set GCS bucket for temporary files.
      */
    def setBigQueryGcsBucket(gcsBucket: String): Unit = {
      hadoopConf.set(BigQueryConfiguration.GCS_BUCKET_KEY, gcsBucket)
    }
  }

  /**
    * Enhanced version of DataFrame with BigQuery support.
    */
  implicit class BigQueryDataFrame(df: DataFrame) extends Serializable {

    val adaptedDf = BigQueryAdapter(df)

    @transient
    lazy val hadoopConf = df.sqlContext.sparkContext.hadoopConfiguration

    @transient
    lazy val jsonParser = new JsonParser()

    /**
      * Save DataFrame data into BigQuery table using Hadoop writer API
      *
      * @param fullyQualifiedOutputTableId output-table id of the form
      *                                    [optional projectId]:[datasetId].[tableId]
      */
    def saveAsBigQueryTable(fullyQualifiedOutputTableId: String): Unit = {
      val tableSchema = BigQuerySchema(adaptedDf)

      BigQueryConfiguration.configureBigQueryOutput(hadoopConf, fullyQualifiedOutputTableId, tableSchema)
      hadoopConf.set("mapreduce.job.outputformat.class", classOf[BigQueryOutputFormat[_, _]].getName)

      adaptedDf
        .toJSON
        .rdd
        .map(json => (null, jsonParser.parse(json)))
        .saveAsNewAPIHadoopDataset(hadoopConf)
    }

    /**
      * Save DataFrame data into BigQuery table using streaming API
      *
      * @param fullyQualifiedOutputTableId output-table id of the form
      *                                    [optional projectId]:[datasetId].[tableId]
      * @param batchSize                   number of rows to write to BigQuery at once
      *                                    (default: 500)
      */
    def streamToBigQueryTable(fullyQualifiedOutputTableId: String, batchSize: Int = 500): Unit = {
      adaptedDf
        .toJSON
        .writeStream
        .bigQueryTable(fullyQualifiedOutputTableId)
    }
  }
}
