import com.appsflyer.spark.bigquery.BigQueryClient
import com.google.api.services.bigquery.Bigquery
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FeatureSpec, GivenWhenThen}
import org.mockito.Mockito._
import org.scalatest.Matchers._

/**
  * Created by root on 1/12/17.
  */
class BigQueryClientSpecs extends FeatureSpec with GivenWhenThen with DataFrameSuiteBase {
  val bigQueryMock =  mock(classOf[Bigquery])

  scenario("When converting a simple dataframe") {
    val sqlCtx = sqlContext
    Given("A sql query")
    val bigQueryClient =  new BigQueryClient(sqlCtx, bigQueryMock)
    val sampleJson = """{
                       |	"id": 1,
                       |	"error": null
                       |}""".stripMargin
    val expectedDF = sqlCtx.read.json(sc.parallelize(List(sampleJson)))
    val sqlQuery = "SELECT * FROM test-table"

    When("querying big query table")
    val actualDF = bigQueryClient.query(sqlQuery)

    Then("We should receive a dataframe")
    actualDF should not be null
    actualDF should be (expectedDF)

  }
}
