import com.appsflyer.spark.bigquery.{BigQueryClient, BigQuerySchema}
import com.google.api.services.bigquery.Bigquery
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}
import org.mockito.Mockito._


class BigQueryClientSpecs extends FeatureSpec with GivenWhenThen with DataFrameSuiteBase {

  val bigQueryMock =  mock(classOf[Bigquery])
  val bigQueryClient =  new BigQueryClient(sqlContext, bigQueryMock)

  feature("Big Query Client") {
    scenario("When loading data from BQ") {
      Given("A sql query")
      val sampleJson = """{
                         |	"id": 1,
                         |	"error": null
                         |}""".stripMargin
      val expectedDF = sqlContext.read.json(sc.parallelize(List(sampleJson)))
      val sqlQuery = "SELECT * FROM test-table"

      When("querying big query table")
      val actualDF = bigQueryClient.query(sqlQuery)

      Then("We should receive a dataframe")
      actualDF should not be null
      actualDF should be (expectedDF)

    }

  }
}