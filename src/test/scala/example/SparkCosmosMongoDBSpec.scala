package example

import org.scalatest._

class SparkCosmosMongoDBSpec extends FlatSpec with Matchers {
  "The SparkCosmosMongoDB object" should "say hello" in {
    SparkCosmosMongoDB.greeting shouldEqual "hello"
  }
}
