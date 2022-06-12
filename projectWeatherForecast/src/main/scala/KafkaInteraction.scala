import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{StringType, StructType}

class KafkaInteraction(){
  val bootstrapServers: String = "localhost:29092"

  def getDataSpark(topic: String): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
    df.printSchema()
    val testStringDF = df.selectExpr("CAST(value AS STRING)")
    val schema = new StructType()
      .add("city", StringType)
      .add("temperature", StringType)
      .add("time_sky", StringType)

    val testDF = testStringDF.select(from_json(col("value"), schema).as("data"))
      .select("data.*")
    testDF.writeStream.format("console").outputMode("append").start().awaitTermination()
  }
}

object test extends App{
  val kafkaInteraction: KafkaInteraction = new KafkaInteraction()
  kafkaInteraction.getDataSpark("raw_datas")
}
