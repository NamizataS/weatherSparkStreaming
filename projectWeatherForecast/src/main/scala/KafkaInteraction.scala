import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, concat, date_format, from_json, lit, split, struct, to_json, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

class KafkaInteraction(){
  val bootstrapServers: String = "localhost:29092"

  def getDataSpark(topic: String): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", value = false)
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    val toStringDF = df.selectExpr("CAST(value AS STRING)", "timestamp")
    val schema = new StructType()
      .add("city", StringType)
      .add("country", StringType)
      .add("temperature", StringType)
      .add("time_sky", StringType)
      .add("location", StringType)

    val rawDF = toStringDF.select(from_json(col("value"), schema).as("data"), col("timestamp"))
      .select("data.*", "timestamp")
    val cleanTimeSkyDF = rawDF.withColumn("time_sky", split(col("time_sky"), "\n"))
      .withColumn("time", col("time_sky").getItem(0))
      .withColumn("sky_status", col("time_sky").getItem(1))
      .drop(col("time_sky"))

    val cleanTimestampDF = cleanTimeSkyDF.withColumn("day", date_format(col("timestamp"), "d"))
      .withColumn("month", date_format(col("timestamp"), "MMMM"))
      .withColumn("year", date_format(col("timestamp"), "yyyy"))
      .withColumn("day_string", date_format(col("timestamp"), "EEEE"))
      .withColumn("time_split", split(col("time"), " "))
      .withColumn("date_formatted", concat(col("day_string"),
        lit(" "), col("day"), lit(" "), col("month"), lit(" "),
        col("year"), lit(" "), col("time_split").getItem(1)))

    val temperatureFormattedDF = cleanTimestampDF.withColumn("temperatureFormatted", split(col("temperature"), "°"))
      .withColumn("temperatureFormatted", col("temperatureFormatted").getItem(0))
      .withColumn("temperatureFormatted", col("temperatureFormatted").cast(IntegerType))

    val splitLatLongDF = temperatureFormattedDF.withColumn("splitLatLng", split(col("location"), ","))
      .withColumn("lat", col("splitLatLng").getItem(0))
      .withColumn("lng", col("splitLatLng").getItem(1))
    val cleanDF = splitLatLongDF
      .drop("day", "month", "year", "day_string", "time_split", "splitLatLng")

    cleanDF.printSchema()

    val avgTemperatureDF = cleanDF
      .select("city", "country", "lat", "lng", "temperatureFormatted", "timestamp")
      .withWatermark("timestamp", "5 minutes")
      .groupBy(col("city"), col("country"), col("lat"), col("lng"),
                window(col("timestamp"), "5 minutes"))
      .avg("temperatureFormatted")

    cleanDF.writeStream
      .trigger(Trigger.ProcessingTime("5 minutes"))
      .format("console")
      .outputMode("append")
      .start()

    avgTemperatureDF.printSchema()
    avgTemperatureDF.select(to_json(struct("*")).as("value"))
      .selectExpr("CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("checkpointLocation", "./newCheckpoints")
      .option("topic", "avg_weather")
      .start()
    avgTemperatureDF.writeStream.format("console").start().awaitTermination()
  }
}

object test extends App{
  val kafkaInteraction: KafkaInteraction = new KafkaInteraction()
  kafkaInteraction.getDataSpark("raw_datas")
}
