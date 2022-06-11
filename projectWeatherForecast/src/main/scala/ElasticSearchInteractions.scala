import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties, RequestFailure, RequestSuccess}
import com.sksamuel.elastic4s.requests.searches.SearchResponse
import org.elasticsearch.spark._

class ElasticSearchInteractions {
  val props: ElasticProperties = ElasticProperties("http://localhost:9200/")
  val client: ElasticClient = ElasticClient(JavaClient(props))
  import com.sksamuel.elastic4s.ElasticDsl._
  def test(): Unit ={
    val resp = client.execute{
      search("cities_index").size(0).aggs{
        termsAgg("cities", "city")
      }.size(42906)
    }.await
    resp match {
      case failure: RequestFailure => println("Becca is a clown " + failure.error)
      case results: RequestSuccess[SearchResponse] => {
        val typeRecord= results.result.hits.hits.toList
        println(typeRecord)
      }
      case results: RequestSuccess[_] => println(results.result)
    }
  }

  def close_connection(): Unit ={
    client.close()
  }
}

object main extends App{
  val esClient = new ElasticSearchInteractions
  esClient.test()
  esClient.close_connection()
}
