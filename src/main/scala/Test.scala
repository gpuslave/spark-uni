import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]): Unit = {
    val logFile = "/app/src/TEST.txt"
    // val logFile = "/opt/spark/work-dir/src/TEST.txt"
    val spark = SparkSession
      .builder
      .appName("App")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    val logData = spark
      .read
      .textFile(logFile)
      .cache()

    val numAs = logData
      .filter(line => line.contains("a"))
      .count()

    val numBs = logData
      .filter(line => line.contains("b"))
      .count()

    println(s"Lines with a: $numAs, Lines with b: $numBs")

    spark.stop()
  }
}

object Aboba {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("App")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    println(s"ABOBA")

    spark.stop()
  }
}