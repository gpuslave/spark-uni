// object App {
//   def main(args: Array[String]): Unit = {
//     val logFile = "/app/src/TEST.txt"
//     // val logFile = "/opt/spark/work-dir/src/TEST.txt"
//     val spark = SparkSession
//       .builder
//       .appName("App")
//       .getOrCreate()
    
//     spark.sparkContext.setLogLevel("ERROR")
    
//     val logData = spark
//       .read
//       .textFile(logFile)
//       .cache()

//     val numAs = logData
//       .filter(line => line.contains("a"))
//       .count()

//     val numBs = logData
//       .filter(line => line.contains("b"))
//       .count()

//     println(s"Lines with a: $numAs, Lines with b: $numBs")

//     spark.stop()
//   }
// }

// object RDD {
//   private val conf = new SparkConf()
//     .setAppName("RDD")
//     .setMaster("local")
//   private val sc = new SparkContext(conf)

//   def preConfig(): Unit = {
//     sc.setLogLevel("ERROR")
//     println("\n")
//   }

//   def main(args: Array[String]): Unit = {
//     preConfig()

//     val data = Array(1,2,3,4,5)
//     val dData = sc.parallelize(data, 4)
//     dData.foreach(println)

//     // -----
//     println("\n")
//     // -----

//     val txtFilePath   = "/app/src/TEST.txt"
//     val distFile      = sc.textFile(txtFilePath)
//       .filter(_.nonEmpty)

//     val lineLengths   = distFile.map(_.length)
//     lineLengths.foreach(println) // works great in local mode, use rdd.collect().f() or rdd.take(123).f()

//     val totalLength   = lineLengths.reduce(_+_)
//     println(totalLength)

//     // -----
//     println("\n")
//     // -----

//     val lnFilePath = "/app/src/same_lines.txt"
//     val lnFile = sc.textFile(lnFilePath)
//       .filter(_.nonEmpty)

//     val pairs = lnFile
//       .map((_, 1))
//     println("Pairs:")
//     pairs.foreach(println)

//     val counts = pairs
//       .reduceByKey(_+_)
//     println("ReduceByKey of pairs:")
//     counts.foreach(println)

//     sc.stop()
//   }
// }