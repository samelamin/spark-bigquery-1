package com.appsflyer.spark.bigquery.streaming

import com.appsflyer.spark.bigquery.BigQueryServiceFactory
import com.appsflyer.spark.utils.BigQueryPartitionUtils
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.services.bigquery.Bigquery
import com.google.api.services.bigquery.model.{Table, TableDataInsertAllRequest, TableReference, TimePartitioning}
import com.google.cloud.hadoop.io.bigquery.BigQueryStrings
import com.google.gson.Gson
import org.apache.spark.sql.ForeachWriter
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.util.control.NonFatal


/**
  * Writes streaming data into BigQuery
  *
  * @param fullyQualifiedOutputTableId output-table id of the form
  *                                    [optional projectId]:[datasetId].[tableId]
  * @param batchSize                   number of rows to write to BigQuery at once
  */
class BigQueryStreamWriter(fullyQualifiedOutputTableId: String, batchSize: Int,
                           isPartitionedByDay: Boolean = false) extends ForeachWriter[String] {

  @transient
  lazy val targetTable: TableReference = BigQueryStrings.parseTableReference(fullyQualifiedOutputTableId)
  private val logger: Logger = LoggerFactory.getLogger(classOf[BigQueryStreamWriter])

  @transient
  var batchId: Long = 0
  val DEFAULT_TABLE_EXPIRATION_MS = 259200000L

  @transient
  var bqService: Bigquery = null

  // Pre-fill rows buffer in order to reuse it
  @transient
  lazy val rows = Array.fill[TableDataInsertAllRequest.Rows](batchSize)(new TableDataInsertAllRequest.Rows)

  @transient
  var rowIndex = 0

  @transient
  lazy val gson = new Gson

  @transient
  lazy val targetClass = (new java.util.HashMap[String, Object]).getClass

  override def open(partitionId: Long, batchId: Long): Boolean = {
    this.batchId = batchId
    this.bqService = BigQueryServiceFactory.getService
    true
  }

  override def process(value: String): Unit = {
    if(isPartitionedByDay) {
      BigQueryPartitionUtils.createBigQueryPartitionedTable(targetTable)
    }
    if (rowIndex < batchSize) {
      rows(rowIndex).setJson(gson.fromJson(value, targetClass)).setInsertId(s"${batchId}_${rowIndex}")
      rowIndex = rowIndex + 1
    } else {
      try {
        bqService.tabledata().insertAll(
          targetTable.getProjectId,
          targetTable.getDatasetId,
          targetTable.getTableId,
          new TableDataInsertAllRequest().setRows(rows.toList)
        )
      } finally {
        rowIndex = 0
      }
    }
  }

  override def close(errorOrNull: Throwable): Unit = {
  }
}
