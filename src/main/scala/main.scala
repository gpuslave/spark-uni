import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

// ----------------------- Лаба 1 -----------------------
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
      .foreach(pair => (pair._1.foreach(print), println("\t" + pair._2)))

    println()
    // 2. Посчитать количество вхождений
    val countWTF = linesWithWTF
      .count() // return RDD size

    println(
      "The amount of \"" + wordToFind + "\" word in this file is - " + countWTF
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
    rbk.collect().foreach { case (k, v) => println(s"$k -> $v") }

    println()

    val mk = aRDD
      .map { case (key, value) => (key, value - 300) }
    mk.collect().foreach { case (k, v) => println(s"$k -> $v") }

    sc.stop()
  }
}

// ----------------------- Практика 1 -----------------------
object pracOne {

  private val conf = new SparkConf()
    .setAppName("pracOne") // Изменено имя приложения для соответствия объекту
    .setMaster(
      "local[*]"
    ) // Используем local[*] для использования всех доступных ядер

  private val sc = new SparkContext(conf)

  def preConfig(): Unit = {
    sc.setLogLevel("ERROR")
  }

  def main(args: Array[String]): Unit = {
    preConfig()

    println("--- Практика RDD ---")

    println("\n=== Раздел 1: Параллельные коллекции ===")
    val data1 = Array(1, 2, 3, 4, 5)
    val distData: RDD[Int] = sc.parallelize(data1) // [slide: 1]
    println(s"Исходные данные (коллекция): ${data1.mkString(", ")}")

    val sum = distData.reduce((a, b) => a + b) // [slide: 1]
    println(s"Сумма элементов RDD: $sum") // Вывод: 15

    println()

    println("\n=== Раздел 2: Внешние наборы данных (Пример с HDFS) ===")
    val filePath = "/app/src/nemtsev.txt" // [slide: 2]
    println(s"Чтение файла из: $filePath")

    val rddFromFile = sc.textFile(filePath) // [slide: 2]
    val wordRdd = rddFromFile.flatMap(_.split(" ")) // [slide: 2]

    val kvRdd = wordRdd.map((_, 1)) // [slide: 2]
    val wordCountRdd = kvRdd.reduceByKey(_ + _) // [slide: 2]
    println("Подсчет слов (первые 10):")
    wordCountRdd
      .take(10)
      .foreach(println) // [slide: 2] (collect может быть большим)

    val outputPath = "/opt/spark/work-dir/pracOneOutput" // [slide: 2]
    println(s"Сохранение результата в: $outputPath")
    wordCountRdd.saveAsTextFile(outputPath) // [slide: 2]

    println()

    // === Раздел 3: Работа с RDD (map, sortBy, filter, collect) ===
    println("\n=== Раздел 3: Работа с RDD (map, sortBy, filter) ===")
    val rdd_p3_1 =
      sc.parallelize(List(5, 6, 4, 7, 3, 8, 2, 9, 1, 10)) // [slide: 3]
    println(s"Исходный RDD: ${rdd_p3_1.collect().mkString(", ")}")
    // Умножаем каждый элемент на 2 и сортируем
    val rdd_p3_2 =
      rdd_p3_1.map(_ * 2).sortBy(x => x, ascending = true) // [slide: 3]
    println(s"RDD после map(*2) и sortBy: ${rdd_p3_2.collect().mkString(", ")}")
    // Фильтруем элементы >= 10
    val rdd_p3_3 = rdd_p3_2.filter(_ >= 10) // [slide: 3]
    println(
      s"RDD после filter(>=10): ${rdd_p3_3.collect().mkString(", ")}"
    ) // [slide: 3]

    println()

    // === Раздел 4: Работа с RDD (flatMap) ===
    println("\n=== Раздел 4: Работа с RDD (flatMap) ===")
    val rdd_p4_1 =
      sc.parallelize(Array("a b c", "d e f", "h i j")) // [slide: 4]
    println(s"Исходный RDD (строки): ${rdd_p4_1.collect().mkString(" | ")}")
    // Разбиваем строки на слова
    val rdd_p4_2 = rdd_p4_1.flatMap(_.split(' ')) // [slide: 4]
    println(
      s"RDD после flatMap: ${rdd_p4_2.collect().mkString(", ")}"
    ) // [slide: 4]

    println()

    // === Раздел 5: Работа с RDD (union, intersection, distinct) ===
    println("\n=== Раздел 5: Работа с RDD (union, intersection, distinct) ===")
    val rdd_p5_1 = sc.parallelize(List(5, 6, 4, 3)) // [slide: 5]
    val rdd_p5_2 = sc.parallelize(List(1, 2, 3, 4)) // [slide: 5]
    println(s"RDD 1: ${rdd_p5_1.collect().mkString(", ")}")
    println(s"RDD 2: ${rdd_p5_2.collect().mkString(", ")}")
    // Объединение
    val rdd_p5_3_union = rdd_p5_1.union(rdd_p5_2) // [slide: 5]
    println(s"Объединение (union): ${rdd_p5_3_union.collect().mkString(", ")}")
    // Уникальные элементы объединения
    val rdd_p5_distinct = rdd_p5_3_union.distinct() // [slide: 5]
    println(
      s"Уникальные элементы объединения (distinct): ${rdd_p5_distinct.collect().mkString(", ")}"
    )
    // Пересечение
    val rdd_p5_4_intersection = rdd_p5_1.intersection(rdd_p5_2) // [slide: 5]
    println(
      s"Пересечение (intersection): ${rdd_p5_4_intersection.collect().mkString(", ")}"
    ) // [slide: 5]

    println()

    // === Раздел 6: Работа с RDD (join, union, groupByKey) ===
    println("\n=== Раздел 6: Работа с RDD (join, union, groupByKey) ===")
    val rdd_p6_1 =
      sc.parallelize(List(("tom", 1), ("jerry", 3), ("kitty", 2))) // [slide: 6]
    val rdd_p6_2 =
      sc.parallelize(List(("jerry", 2), ("tom", 1), ("shuke", 2))) // [slide: 6]
    println(s"RDD 1 (пары): ${rdd_p6_1.collect().mkString(", ")}")
    println(s"RDD 2 (пары): ${rdd_p6_2.collect().mkString(", ")}")
    // Соединение (Join)
    val rdd_p6_3_join = rdd_p6_1.join(rdd_p6_2) // [slide: 6]
    println(
      s"Соединение (join): ${rdd_p6_3_join.collect().mkString(", ")}"
    ) // [slide: 6]
    // Объединение (Union) для пар
    val rdd_p6_4_union = rdd_p6_1.union(rdd_p6_2) // [slide: 6]
    println(
      s"Объединение пар (union): ${rdd_p6_4_union.collect().mkString(", ")}"
    )
    // Группировка по ключу
    val rdd_p6_grouped = rdd_p6_4_union.groupByKey() // [slide: 6]
    // Преобразуем результат groupByKey для читаемого вывода
    val groupedResult = rdd_p6_grouped
      .map { case (key, values) => (key, values.toList) }
      .collect()
    println(
      s"Группировка по ключу (groupByKey): ${groupedResult.mkString(", ")}"
    ) // [slide: 6] (collect вызывался на RDD до map)

    println()

    // === Раздел 7: Работа с RDD (cogroup) ===
    println("\n=== Раздел 7: Работа с RDD (cogroup) ===")
    val rdd_p7_1 = sc.parallelize(
      List(("tom", 1), ("tom", 2), ("jerry", 3), ("kitty", 2))
    ) // [slide: 7]
    val rdd_p7_2 =
      sc.parallelize(List(("jerry", 2), ("tom", 1), ("shuke", 2))) // [slide: 7]
    println(s"RDD 1 (пары): ${rdd_p7_1.collect().mkString(", ")}")
    println(s"RDD 2 (пары): ${rdd_p7_2.collect().mkString(", ")}")
    // Cogroup
    val rdd_p7_3_cogroup = rdd_p7_1.cogroup(rdd_p7_2) // [slide: 7]
    // Преобразуем результат cogroup для читаемого вывода
    val cogroupResult = rdd_p7_3_cogroup
      .map { case (key, (iter1, iter2)) => (key, (iter1.toList, iter2.toList)) }
      .collect()
    println(s"Результат cogroup: ${cogroupResult.mkString(", ")}") // [slide: 7]

    println()

    // === Раздел 8: Работа с RDD (reduce) ===
    println("\n=== Раздел 8: Работа с RDD (reduce) ===")
    val rdd_p8_1 = sc.parallelize(List(1, 2, 3, 4, 5)) // [slide: 8]
    println(s"Исходный RDD: ${rdd_p8_1.collect().mkString(", ")}")
    // (Reduce)
    val rdd_p8_2_reduce_result = rdd_p8_1.reduce(_ + _) // [slide: 8]
    println(s"Результат reduce (+): $rdd_p8_2_reduce_result") // [slide: 8]

    println()

    // === Раздел 9: Работа с RDD (union, reduceByKey, sortByKey) ===
    println("\n=== Раздел 9: Работа с RDD (union, reduceByKey, sortByKey) ===")
    val rdd_p9_1 = sc.parallelize(
      List(("tom", 1), ("jerry", 3), ("kitty", 2), ("shuke", 1))
    ) // [slide: 9]
    val rdd_p9_2 = sc.parallelize(
      List(("jerry", 2), ("tom", 3), ("shuke", 2), ("kitty", 5))
    ) // [slide: 9]
    println(s"RDD 1 (пары): ${rdd_p9_1.collect().mkString(", ")}")
    println(s"RDD 2 (пары): ${rdd_p9_2.collect().mkString(", ")}")
    // Объединение
    val rdd_p9_3_union = rdd_p9_1.union(rdd_p9_2) // [slide: 9]
    println(
      s"Объединение пар (union): ${rdd_p9_3_union.collect().mkString(", ")}"
    )
    // Агрегация по ключу
    val rdd_p9_4_reduced = rdd_p9_3_union.reduceByKey(_ + _) // [slide: 9]
    println(
      s"Агрегация по ключу (reduceByKey +): ${rdd_p9_4_reduced.collect().mkString(", ")}"
    ) // [slide: 9]
    // Сортировка по значению (в порядке убывания)
    // Шаг 1: Меняем местами ключ и значение (значение становится ключом)
    // Шаг 2: Сортируем по новому ключу (бывшему значению) в убывающем порядке
    // Шаг 3: Меняем ключ и значение обратно
    val rdd_p9_5_sorted = rdd_p9_4_reduced
      .map(t => (t._2, t._1)) // значение -> ключ
      .sortByKey(ascending = false) // сортируем по значению (убыв.)
      .map(t => (t._2, t._1)) // ключ -> значение (обратно)  [slide: 9]
    println(
      s"Сортировка по убыванию значения: ${rdd_p9_5_sorted.collect().mkString(", ")}"
    ) // [slide: 9]

    println("\n--- Практика завершена ---")

    // Останавливаем SparkContext
    sc.stop()
  }
}
// TODO: углубиться что такое join, groupByKey, cogroup на парах (_,_)

// ----------------------- Практика 2 -----------------------
object pracTwo {

  private val conf = new SparkConf()
    .setAppName("pracTwo") // Имя приложения
    .setMaster(
      "local[*]"
    ) // Запуск в локальном режиме со всеми доступными ядрами

  private val sc = new SparkContext(conf)

  def preConfig(): Unit = {
    sc.setLogLevel(
      "ERROR"
    ) // Устанавливаем уровень логирования, чтобы избежать лишнего вывода
    println("\n")
  }

  def main(args: Array[String]): Unit = {
    preConfig()

    val csvFilePath = "/app/src/data.csv"

    println("--- Практика 2: RDD Transformations ---")
    val rdd = sc
      .textFile(csvFilePath)
      .filter(_.nonEmpty)
      .cache()

    rdd.collect().foreach(println)
    val rdd1 = rdd
      .flatMap(_.split(","))
    rdd1.foreach(print)
  }
}
