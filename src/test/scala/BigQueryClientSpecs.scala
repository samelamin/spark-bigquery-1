import com.appsflyer.spark.bigquery.BigQueryClient
import com.google.api.services.bigquery.Bigquery
import com.google.api.services.bigquery.model.Dataset
import com.google.api.services.bigquery.model.DatasetList.Datasets
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FeatureSpec
import org.mockito.Mockito._
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar
import org.mockito.Matchers.any
/**
  * Created by root on 1/12/17.
  */
class BigQueryClientSpecs extends FeatureSpec with DataFrameSuiteBase with MockitoSugar  {

  scenario("When converting a simple dataframe") {
    val bigQueryMock =  mock[Bigquery](RETURNS_DEEP_STUBS)
    val sqlCtx = sqlContext
    val bigQueryClient =  new BigQueryClient(sqlCtx, bigQueryMock)
    when(bigQueryMock.datasets().get(any[String],any[String]).execute()).thenReturn(new Dataset)
    val sqlQuery = "SELECT * FROM test-table"
    val actualDF = bigQueryClient.query(sqlQuery)

  }
}
