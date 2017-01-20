import java.io.File

import com.appsflyer.spark.bigquery.{BigQueryAdapter, BigQueryClient, BigQuerySchema}
import com.google.api.services.bigquery.Bigquery
import com.google.api.services.bigquery.model._
import com.google.cloud.hadoop.io.bigquery._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.hadoop.io.LongWritable
import org.scalatest.FeatureSpec
import org.mockito.Mockito._
import org.mockito.Matchers.{any, eq => mockitoEq}
import org.scalatest.mock.MockitoSugar
import com.google.gson.{JsonObject, JsonParser}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.sql._
/**
  * Created by root on 1/12/17.
  */
class BigQueryClientSpecs extends FeatureSpec with DataFrameSuiteBase with MockitoSugar {
  val jobProjectId = "google.com:foo-project"

  def setupBigQueryClient(sqlCtx: SQLContext, bigQueryMock: Bigquery): BigQueryClient = {
    val fakeJobReference = new JobReference()
    fakeJobReference.setProjectId(jobProjectId)
    fakeJobReference.setJobId("bigquery-job-1234")
    val dataProjectId = "publicdata"
    // Create the job result.
    val jobStatus = new JobStatus()
    jobStatus.setState("DONE")
    jobStatus.setErrorResult(null)

    val jobHandle = new Job()
    jobHandle.setStatus(jobStatus)
    jobHandle.setJobReference(fakeJobReference)

    // Create table reference.
    val tableRef = new TableReference()
    tableRef.setProjectId(dataProjectId)
    tableRef.setDatasetId("test_dataset")
    tableRef.setTableId("test_table")

    // Mock getting Bigquery jobs
    when(bigQueryMock.jobs().get(any[String], any[String]).execute())
      .thenReturn(jobHandle)
    when(bigQueryMock.jobs().insert(any[String], any[Job]).execute())
      .thenReturn(jobHandle)

    val bigQueryClient = new BigQueryClient(sqlCtx, bigQueryMock)
    bigQueryClient
  }

  scenario("When writing to BQ") {
    val sqlCtx = sqlContext
    val gcsPath = "/tmp/testfile2.json"
    FileUtils.deleteQuietly(new File(gcsPath))
    val sampleJson = """{
                       |	"id": 1,
                       |	"error": null
                       |}""".stripMargin
    val df = sqlContext.read.json(sc.parallelize(List(sampleJson)))
    val bigQueryMock =  mock[Bigquery](RETURNS_DEEP_STUBS)
    val fullyQualifiedOutputTableId = "testProjectID:test_dataset.test"
    val targetTable = BigQueryStrings.parseTableReference(fullyQualifiedOutputTableId)
    val bigQueryClient = setupBigQueryClient(sqlCtx, bigQueryMock)
    bigQueryClient.load(gcsPath,targetTable)
    verify(bigQueryMock.jobs().insert(mockitoEq(jobProjectId),any[Job]), times(1)).execute()
  }


  scenario("When reading from BQ") {
    val sqlCtx = sqlContext
    val hadoopConf = sqlCtx.sparkContext.hadoopConfiguration
    val gcsPath = "testfile2.json"
    FileUtils.deleteQuietly(new File(gcsPath))
    val sampleJson = """{
                       |	"id": 1,
                       |	"error": null
                       |}""".stripMargin
    val df = sqlContext.read.json(sc.parallelize(List(sampleJson)))
    val adaptedDf = BigQueryAdapter(df)
    val bigQueryMock =  mock[Bigquery](RETURNS_DEEP_STUBS)
    lazy val jsonParser = new JsonParser()
    hadoopConf.set("fs.gs.auth.service.account.enable", "false")

    val fullyQualifiedOutputTableId = "testProjectID:test_dataset.test"
    val tableSchema = BigQuerySchema(df)
    BigQueryConfiguration.configureBigQueryOutput(hadoopConf, fullyQualifiedOutputTableId, tableSchema)
    hadoopConf.set(BigQueryConfiguration.TEMP_GCS_PATH_KEY, gcsPath)
    val targetTable = BigQueryStrings.parseTableReference(fullyQualifiedOutputTableId)

    adaptedDf
      .toJSON
      .rdd
      .map(json => (null, jsonParser.parse(json)))
      .saveAsNewAPIHadoopFile(hadoopConf.get(BigQueryConfiguration.TEMP_GCS_PATH_KEY),
        classOf[GsonBigQueryInputFormat],
        classOf[LongWritable],
        classOf[TextOutputFormat[NullWritable, JsonObject]],
        hadoopConf)

    val bigQueryClient = setupBigQueryClient(sqlCtx, bigQueryMock)
    hadoopConf.set(bigQueryClient.STAGING_DATASET_LOCATION,"US")

//    val actualDF = bigQueryClient.query("select * from test")


//    verify(bigQueryMock.jobs().insert(mockitoEq(jobProjectId),any[Job]), times(1)).execute()


    //    verify(mockBigQueryHelper, times(1))
    //      .insertJobOrFetchDuplicate(eq(jobProjectId), any(Job.class));
    //    val expectedRDD = adaptedDf.rdd.map(row => (row(0),row(1)))
    //

    //    expectedRDD.saveAsNewAPIHadoopFile(
    //      hadoopConf.get(BigQueryConfiguration.TEMP_GCS_PATH_KEY),
    //      classOf[GsonBigQueryInputFormat],
    //      classOf[LongWritable],
    //      classOf[TextOutputFormat[NullWritable, JsonObject]],
    //      hadoopConf)
    //    hadoopConf.set(BigQueryConfiguration.TEMP_GCS_PATH_KEY, gcsPath)
    //    hadoopConf.set(BigQueryConfiguration.PROJECT_ID_KEY, "test project")
    //    hadoopConf.set(BigQueryConfiguration.OUTPUT_PROJECT_ID_KEY, "test project")
    //    hadoopConf.set(BigQueryConfiguration.OUTPUT_DATASET_ID_KEY, "test_dataset")
    //    hadoopConf.set(BigQueryConfiguration.OUTPUT_TABLE_ID_KEY, "test_dataset")
    //    hadoopConf.set(BigQueryConfiguration.OUTPUT_TABLE_ID_KEY, "test_dataset")
    //    hadoopConf.set(BigQueryConfiguration.OUTPUT_TABLE_SCHEMA_KEY, expectedSchema)
    //
    ////
    ////
    //    expectedRDD.saveAsNewAPIHadoopFile(
    //      hadoopConf.get(BigQueryConfiguration.TEMP_GCS_PATH_KEY),
    //      classOf[GsonBigQueryInputFormat],
    //      classOf[LongWritable],
    //      classOf[TextOutputFormat[NullWritable, JsonObject]],
    //      hadoopConf)
    //
    //
    //
    //    //    val tableData = sqlContext.sparkContext.newAPIHadoopRDD(
    //    //      hadoopConf,
    //    //      classOf[GsonBigQueryInputFormat],
    //    //      classOf[LongWritable],
    //    //      classOf[JsonObject]).map(_._2.toString)

    //    when(bigQueryMock.jobs().get(any[String],any[String]).execute()).thenReturn(jobHandle)
    //    val sqlQuery = "SELECT * FROM test-table"
    //    val adaptedDf: DataFrame = BigQueryAdapter(t)
    //
    //    bigQueryClient.load(adaptedDf,"test:test.test", false)
    ////    val actualDF = bigQueryClient.query(sqlQuery)
  }


}
