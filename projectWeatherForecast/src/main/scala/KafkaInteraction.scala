import org.apache.spark.sql.SparkSession

class KafkaInteraction(){
  val bootstrapServers: String = "localhost:29092"

  def getDataSpark(topic: String): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", "raw_datas")
      .option("startingOffsets", "earliest")
      .load()
    val testStringDF = df.selectExpr("CAST(value AS STRING)")
    testStringDF.head()
  }
}

object test extends App{
  val kafkaInteraction: KafkaInteraction = new KafkaInteraction()
  kafkaInteraction.getDataSpark("raw_datas")
}
