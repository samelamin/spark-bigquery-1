
import com.appsflyer.spark.bigquery.BigQueryClient
import com.google.api.services.bigquery.Bigquery
import com.google.api.services.bigquery.model.{Dataset, Job, JobReference}
import com.google.cloud.hadoop.io.bigquery.{BigQueryConfiguration, BigQueryUtils}
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FeatureSpec
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.mockito.Matchers.any
/**
  * Created by root on 1/12/17.
  */
class BigQueryClientSpecs extends FeatureSpec with DataFrameSuiteBase with MockitoSugar  {

  scenario("When reading from BQ") {

    val sampleJson = """{
                       |	"id": 1,
                       |	"error": null
                       |}""".stripMargin
    val expectedRDD = sc.parallelize(List(sampleJson))

    //    val tableData = sqlContext.sparkContext.newAPIHadoopRDD(
    //      hadoopConf,
    //      classOf[GsonBigQueryInputFormat],
    //      classOf[LongWritable],
    //      classOf[JsonObject]).map(_._2.toString)
    val bigQueryMock =  mock[Bigquery](RETURNS_DEEP_STUBS)
    val bigQueryUtilsMock =  mock[BigQueryUtils]
    val sqlCtx = sqlContext
    val hadoopConf = sqlCtx.sparkContext.hadoopConfiguration
    hadoopConf.set(BigQueryConfiguration.PROJECT_ID_KEY, "test project")
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
