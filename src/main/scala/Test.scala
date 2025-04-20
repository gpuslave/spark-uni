import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
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

object RDD {
  private val conf = new SparkConf()
    .setAppName("RDD")
    .setMaster("local")
  private val sc = new SparkContext(conf)

  def preConfig(): Unit = {
    sc.setLogLevel("ERROR")
    println("\n")
  }

  def main(args: Array[String]): Unit = {
    preConfig()

    val data = Array(1,2,3,4,5)
    val dData = sc.parallelize(data, 4)
    dData.foreach(println)

    // -----
    println("\n")
    // -----

    val txtFilePath   = "/app/src/TEST.txt"
    val distFile      = sc.textFile(txtFilePath)
      .filter(_.nonEmpty)

    val lineLengths   = distFile.map(_.length)
    lineLengths.foreach(println) // works great in local mode, use rdd.collect().f() or rdd.take(123).f()

    val totalLength   = lineLengths.reduce(_+_)
    println(totalLength)

    // -----
    println("\n")
    // -----

    val lnFilePath = "/app/src/same_lines.txt"
    val lnFile = sc.textFile(lnFilePath)
      .filter(_.nonEmpty)

    val pairs = lnFile
      .map((_, 1))
    println("Pairs:")
    pairs.foreach(println)

    val counts = pairs
      .reduceByKey(_+_)
    println("ReduceByKey of pairs:")
    counts.foreach(println)

    sc.stop()
  }
}

object taskOne {
  private val conf = new SparkConf()
    .setAppName("taskOne")
    .setMaster("local")

  private val sc = new SparkContext(conf)

  def preConfig() = {
    sc.setLogLevel("ERROR")
    println("\n")
  }

  def main(args: Array[String]): Unit = {
    preConfig()
    val filePath = "/app/src/nemtsev.txt"
    val wordToFind = "line"
      .toLowerCase()

    val RDD = sc
      .textFile(
        filePath,
        4
      ) // load file content as RDD, divide into 4 partitions
      .filter(_.nonEmpty) // remove empty lines from dataset
      .map(_.toLowerCase())
      .cache() // cache this RDD for later reuse
    RDD.collect().foreach(println)

    println()
    // 1. Найти заданное слово
    val linesWithWTF = RDD
      .zipWithIndex() // create numbered pairs like (line1, 1), (line2, 2) ...
      // .map(pair => (pair._1.split("\\W+"), pair._2)) // split lines by whitespaces
      // .filter(pair => pair._1.contains(wordToFind)) // check for wordToFind in splitted lines (Arrays)
      .map { case (line, idx) => (line.split("\\W+"), idx) }
      .filter { case (words, idx) => words.contains(wordToFind) }
      .cache()

    linesWithWTF
      .collect()
      .foreach(pair => (pair._1.foreach(print), println("\n" + pair._2)))

    println()
    // 2. Посчитать количество вхождений
    val countWTF = linesWithWTF
      .count() // return RDD size

    println(
      "The amount of word \"" + wordToFind + "\" in this file is " + countWTF
    )

    // 3. Разбить текст на слова и удалить пустые строки
    val fullText = sc
      .wholeTextFiles(filePath)
      .values // .map{ case (file, content) => content }
      .flatMap(_.split("\\s+").filter(_.nonEmpty))

    fullText
      .collect()
      .foreach(print)

    println()
    // 4. RDD manipulations
    val intArray = Array(1, 2, 3, 4, 5)
    val sumOfSquares = sc
      .parallelize(intArray, 4)
      .map(elem => elem * elem)
      .reduce(_ + _)
    
    println(s"Sum of squares 1...5: $sumOfSquares")

    val aArray = Array(("a", 1), ("b", 9), ("a", 3), ("a", 3))
    val aRDD = sc.parallelize(aArray)
    val rbk = aRDD
      .reduceByKey(_ + _)
    rbk.collect().foreach{ case (k,v) => println(s"$k -> $v") }
    println()
    val mk = aRDD
      .map{ case (key, value) => (key, value - 300)}
    mk.collect().foreach{ case (k,v) => println(s"$k -> $v") }
    
    sc.stop()
  }
}
