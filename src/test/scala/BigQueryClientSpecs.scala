import com.appsflyer.spark.bigquery.BigQueryClient
import com.google.api.services.bigquery.Bigquery
import com.google.api.services.bigquery.model.{Dataset, Job, JobReference}
import com.google.cloud.hadoop.io.bigquery.{BigQueryConfiguration, BigQueryOutputFormat, BigQueryUtils, GsonBigQueryInputFormat}
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.scalatest.FeatureSpec
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.mockito.Matchers.any
import com.google.gson.JsonObject
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat/**
  * Created by root on 1/12/17.
  */
class BigQueryClientSpecs extends FeatureSpec with DataFrameSuiteBase with MockitoSugar  {

  scenario("When reading from BQ") {
    val sqlCtx = sqlContext
    val hadoopConf = sqlCtx.sparkContext.hadoopConfiguration
    val inputTmpDir = "/Users/sam.elamin/IdeaProjects/testfile2"

    val sampleJson = """{
                       |	"id": 1,
                       |	"error": null
                       |}""".stripMargin

    val expectedSchema = """[ {
                           |  "name" : "error",
                           |  "mode" : "NULLABLE",
                           |  "type" : "STRING"
                           |}, {
                           |  "name" : "id",
                           |  "mode" : "NULLABLE",
                           |  "type" : "INTEGER"
                           |} ]
                           |""".stripMargin.trim

    val t = sqlContext.read.json(sc.parallelize(List(sampleJson)))



//    val x = sc.parallelize(List("THIS","ISCOOL")).map(x => (NullWritable.get, new Text(x)))
//    x.saveAsNewAPIHadoopFile(inputTmpDir,
//      classOf[GsonBigQueryInputFormat],
//      classOf[LongWritable],
////      classOf[TextOutputFormat[NullWritable, Text]],
//      classOf[TextOutputFormat[NullWritable, JsonObject]],
//      sc.hadoopConfiguration)

    val expectedRDD = t.rdd.map(row => (row(0),row(1)))
    hadoopConf.set(BigQueryConfiguration.TEMP_GCS_PATH_KEY, inputTmpDir)
    hadoopConf.set(BigQueryConfiguration.PROJECT_ID_KEY, "test project")
    hadoopConf.set(BigQueryConfiguration.OUTPUT_PROJECT_ID_KEY, "test project")
    hadoopConf.set(BigQueryConfiguration.OUTPUT_DATASET_ID_KEY, "test_dataset")
    hadoopConf.set(BigQueryConfiguration.OUTPUT_TABLE_ID_KEY, "test_dataset")
    hadoopConf.set(BigQueryConfiguration.OUTPUT_TABLE_ID_KEY, "test_dataset")
    hadoopConf.set(BigQueryConfiguration.OUTPUT_TABLE_SCHEMA_KEY, expectedSchema)


    expectedRDD.saveAsNewAPIHadoopFile(
      hadoopConf.get(BigQueryConfiguration.TEMP_GCS_PATH_KEY),
      classOf[GsonBigQueryInputFormat],
      classOf[LongWritable],
      classOf[TextOutputFormat[NullWritable, JsonObject]],
      hadoopConf)



    //    val tableData = sqlContext.sparkContext.newAPIHadoopRDD(
    //      hadoopConf,
    //      classOf[GsonBigQueryInputFormat],
    //      classOf[LongWritable],
    //      classOf[JsonObject]).map(_._2.toString)
    val bigQueryMock =  mock[Bigquery](RETURNS_DEEP_STUBS)
    val jobStatus = new com.google.api.services.bigquery.model.JobStatus
    jobStatus.setState("DONE")
    jobStatus.setErrorResult(null)

    val dataSet = new Dataset()
    dataSet.setId("test_dataset")

    val jobReference = new JobReference()
    jobReference.setProjectId("testProjectID")
    val jobHandle = new Job()
    jobHandle.setJobReference(new JobReference)
    jobHandle.setStatus(jobStatus)

    val bigQueryClient =  new BigQueryClient(sqlCtx, bigQueryMock)

    when(bigQueryMock.datasets().get(any[String],any[String]).execute()).thenReturn(dataSet)
    when(bigQueryMock.jobs().insert(any[String],any[Job]).execute()).thenReturn(jobHandle)

    when(bigQueryMock.jobs().get(any[String],any[String]).execute()).thenReturn(jobHandle)
    val sqlQuery = "SELECT * FROM test-table"
    val actualDF = bigQueryClient.query(sqlQuery)
  }
}
