package com.appsflyer.spark.bigquery

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.{Bigquery, BigqueryScopes}
import com.google.api.services.bigquery.model.{Dataset, _}
import com.google.cloud.hadoop.io.bigquery.{BigQueryConfiguration, BigQueryStrings, BigQueryUtils, GsonBigQueryInputFormat}
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.gson.JsonObject
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.util.Progressable
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._
import org.joda.time.Instant
import org.joda.time.format.DateTimeFormat
import org.slf4j.LoggerFactory

import scala.util.Random
import scala.util.control.NonFatal

/**
  * Created by sam elamin on 11/01/2017.
  */
class BigQueryClient(sqlContext: SQLContext, bigquery: Bigquery = null ) {
  val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
  val STAGING_DATASET_PREFIX = "bq.staging_dataset.prefix"
  val STAGING_DATASET_PREFIX_DEFAULT = "spark_bigquery_staging_"
  val STAGING_DATASET_LOCATION = "bq.staging_dataset.location"
  val STAGING_DATASET_LOCATION_DEFAULT = "US"
  val STAGING_DATASET_TABLE_EXPIRATION_MS = 86400000L
  val STAGING_DATASET_DESCRIPTION = "Spark BigQuery staging dataset"
  val DEFAULT_TABLE_EXPIRATION_MS = 259200000L
  private val logger = LoggerFactory.getLogger(classOf[BigQueryClient])
  private val SCOPES = List(BigqueryScopes.BIGQUERY).asJava

  if(bigquery == null) {
    val bigquery: Bigquery =
    {
      val credential = GoogleCredential.getApplicationDefault.createScoped(SCOPES)
      new Bigquery.Builder(new NetHttpTransport, new JacksonFactory, credential)
        .setApplicationName("spark-bigquery")
        .build()
    }
  }

  private def projectId = hadoopConf.get(BigQueryConfiguration.PROJECT_ID_KEY)
  private val queryCache: LoadingCache[String, TableReference] =
    CacheBuilder.newBuilder()
      .expireAfterWrite(STAGING_DATASET_TABLE_EXPIRATION_MS, TimeUnit.MILLISECONDS)
      .build[String, TableReference](new CacheLoader[String, TableReference] {
      override def load(key: String): TableReference = {
        val sqlQuery = key
        logger.info(s"Executing query $sqlQuery")

        val location = hadoopConf.get(STAGING_DATASET_LOCATION, STAGING_DATASET_LOCATION_DEFAULT)
        val destinationTable = temporaryTable(location)
        val tableName = BigQueryStrings.toString(destinationTable)
        logger.info(s"Destination table: $destinationTable")

        val job = createQueryJob(sqlQuery, destinationTable, dryRun = false)
        waitForJob(job)
        destinationTable
      }
    })

  private def inConsole = Thread.currentThread().getStackTrace.exists(
    _.getClassName.startsWith("scala.tools.nsc.interpreter."))
  private val PRIORITY = if (inConsole) "INTERACTIVE" else "BATCH"
  private val TABLE_ID_PREFIX = "spark_bigquery"
  private val JOB_ID_PREFIX = "spark_bigquery"
  private val TIME_FORMATTER = DateTimeFormat.forPattern("yyyyMMddHHmmss")

  /**
    * Perform a BigQuery SELECT query and save results to a temporary table.
    */
  def query(sqlQuery: String): DataFrame = {
    val tableReference = queryCache.get(sqlQuery)
    val fullyQualifiedInputTableId = BigQueryStrings.toString(tableReference)
    BigQueryConfiguration.configureBigQueryInput(hadoopConf, fullyQualifiedInputTableId)

    val tableData = sqlContext.sparkContext.newAPIHadoopRDD(
      hadoopConf,
      classOf[GsonBigQueryInputFormat],
      classOf[LongWritable],
      classOf[JsonObject]).map(_._2.toString)

    val df = sqlContext.read.json(tableData)
    df
  }

  private def waitForJob(job: Job): Unit = {
    BigQueryUtils.waitForJobCompletion(bigquery, projectId, job.getJobReference, new Progressable {
      override def progress(): Unit = {}
    })
  }

  private def stagingDataset(location: String): DatasetReference = {
    // Create staging dataset if it does not already exist
    val prefix = hadoopConf.get(STAGING_DATASET_PREFIX, STAGING_DATASET_PREFIX_DEFAULT)
    val datasetId = prefix + location.toLowerCase
    try {
      bigquery.datasets().get(projectId, datasetId).execute()
      logger.info(s"Staging dataset $projectId:$datasetId already exists")
    } catch {
      case e: GoogleJsonResponseException if e.getStatusCode == 404 =>
        logger.info(s"Creating staging dataset $projectId:$datasetId")
        val dsRef = new DatasetReference().setProjectId(projectId).setDatasetId(datasetId)
        val ds = new  com.google.api.services.bigquery.model.Dataset()
          .setDatasetReference(dsRef)
          .setDefaultTableExpirationMs(STAGING_DATASET_TABLE_EXPIRATION_MS)
          .setDescription(STAGING_DATASET_DESCRIPTION)
          .setLocation(location)
        bigquery
          .datasets()
          .insert(projectId, ds)
          .execute()

      case NonFatal(e) => throw e
    }
    new DatasetReference().setProjectId(projectId).setDatasetId(datasetId)
  }

  private def temporaryTable(location: String): TableReference = {
    val now = Instant.now().toString(TIME_FORMATTER)
    val tableId = TABLE_ID_PREFIX + "_" + now + "_" + Random.nextInt(Int.MaxValue)
    new TableReference()
      .setProjectId(projectId)
      .setDatasetId(stagingDataset(location).getDatasetId)
      .setTableId(tableId)
  }

  private def createQueryJob(sqlQuery: String,
                             destinationTable: TableReference,
                             dryRun: Boolean): Job = {
    var queryConfig = new JobConfigurationQuery()
      .setQuery(sqlQuery)
      .setPriority(PRIORITY)
      .setCreateDisposition("CREATE_IF_NEEDED")
      .setWriteDisposition("WRITE_EMPTY")
    if (destinationTable != null) {
      queryConfig = queryConfig
        .setDestinationTable(destinationTable)
        .setAllowLargeResults(true)
    }

    val jobConfig = new JobConfiguration().setQuery(queryConfig).setDryRun(dryRun)
    val jobReference = createJobReference(projectId, JOB_ID_PREFIX)
    val job = new Job().setConfiguration(jobConfig).setJobReference(jobReference)
    bigquery.jobs().insert(projectId, job).execute()
  }

  private def createJobReference(projectId: String, jobIdPrefix: String): JobReference = {
    val fullJobId = projectId + "-" + UUID.randomUUID().toString
    new JobReference().setProjectId(projectId).setJobId(fullJobId)
  }

}
