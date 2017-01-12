package com.appsflyer.spark

import com.appsflyer.spark.bigquery.BigQueryClient
import com.appsflyer.spark.bigquery.streaming._
import com.appsflyer.spark.utils.BigQueryPartitionUtils
import com.google.api.services.bigquery.model.TableReference
import com.google.cloud.hadoop.io.bigquery.{BigQueryConfiguration, BigQueryOutputFormat}
import com.google.gson.JsonParser
import org.apache.spark.sql._
import com.google.cloud.hadoop.io.bigquery._
import org.apache.hadoop.io.LongWritable
import com.google.gson.JsonObject

package object bigquery {
  /**
    * Enhanced version of SQLContext with BigQuery support.
    */
  implicit class BigQuerySQLContext(sqlContext: SQLContext) extends Serializable {

    @transient
    lazy val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
    val bq = new BigQueryClient(sqlContext)
    val sc = sqlContext.sparkContext
    val STAGING_DATASET_LOCATION = "bq.staging_dataset.location"

    /**
      * Set GCP project ID for BigQuery.
      */
    def setBigQueryProjectId(projectId: String): Unit = {
      hadoopConf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId)
    }

    def setGSProjectId(projectId: String): Unit = {
      // Also set project ID for GCS connector
      hadoopConf.set("fs.gs.project.id", projectId)
    }

    /**
      * Set GCS bucket for temporary BigQuery files.
      */
    def setBigQueryGcsBucket(gcsBucket: String): Unit =
    hadoopConf.set(BigQueryConfiguration.GCS_BUCKET_KEY, gcsBucket)

    /**
      * Set BigQuery dataset location, e.g. US, EU.
      */
    def setBigQueryDatasetLocation(location: String): Unit =
    hadoopConf.set(STAGING_DATASET_LOCATION, location)

    /**
      * Set GCP JSON key file.
      */
    def setGcpJsonKeyFile(jsonKeyFile: String): Unit = {
      hadoopConf.set("mapred.bq.auth.service.account.json.keyfile", jsonKeyFile)
      hadoopConf.set("fs.gs.auth.service.account.json.keyfile", jsonKeyFile)
    }

    /**
      * Set GCP pk12 key file.
      */
    def setGcpPk12KeyFile(pk12KeyFile: String): Unit = {
      hadoopConf.set("google.cloud.auth.service.account.keyfile", pk12KeyFile)
      hadoopConf.set("mapred.bq.auth.service.account.keyfile", pk12KeyFile)
      hadoopConf.set("fs.gs.auth.service.account.keyfile", pk12KeyFile)
    }
    def bigQuerySelect(sqlQuery: String): DataFrame = bq.query(sqlQuery)
    def bigQueryTable(tableReference: TableReference): DataFrame = {
      val fullyQualifiedInputTableId = BigQueryStrings.toString(tableReference)
      BigQueryConfiguration.configureBigQueryInput(hadoopConf, fullyQualifiedInputTableId)

      val tableData = sc.newAPIHadoopRDD(
        hadoopConf,
        classOf[GsonBigQueryInputFormat],
        classOf[LongWritable],
        classOf[JsonObject]).map(_._2.toString)

      val df = sqlContext.read.json(tableData)
      df
    }
  }

  /**
    * Enhanced version of DataFrame with BigQuery support.
    */
  implicit class BigQueryDataFrame(df: DataFrame) extends Serializable {
    val adaptedDf: DataFrame = BigQueryAdapter(df)

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
    def saveAsBigQueryTable(fullyQualifiedOutputTableId: String, isPartitionedByDay: Boolean = false): Unit = {
      val tableSchema = BigQuerySchema(adaptedDf)

      BigQueryConfiguration.configureBigQueryOutput(hadoopConf, fullyQualifiedOutputTableId, tableSchema)
      hadoopConf.set("mapreduce.job.outputformat.class", classOf[BigQueryOutputFormat[_, _]].getName)
      val bqService = BigQueryServiceFactory.getService
      val targetTable = BigQueryStrings.parseTableReference(fullyQualifiedOutputTableId)

      if(isPartitionedByDay) {
        BigQueryPartitionUtils.createBigQueryPartitionedTable(targetTable,bqService)
      }
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
    def streamToBigQueryTable(fullyQualifiedOutputTableId: String, batchSize: Int = 500,
                              isPartitionedByDay: Boolean = false): Unit = {
      adaptedDf
        .toJSON
        .writeStream
        .bigQueryTable(fullyQualifiedOutputTableId)
    }
  }
}
