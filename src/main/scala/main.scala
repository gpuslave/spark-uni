import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scala.math

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
// === Шаг 2: Преобразование RDD (flatMap) ===
    // Разделяем каждую строку на отдельные слова/элементы по запятой.
    // flatMap преобразует каждую строку в коллекцию слов, а затем "сглаживаем" все эти коллекции в один RDD.
    println("\n=== Шаг 2: RDD после flatMap (_ split \",\") ===")
    val rdd1 = rdd.flatMap { line => line.split(",") }
    // Альтернативный синтаксис: val rdd1 = rdd.flatMap(x => x.split(","))
    println(
      s"Данные после flatMap (отдельные элементы): ${rdd1.collect().mkString(", ")}"
    )
    // Ожидаемый формат: Array[String] = Array(Johnson, Rachel, Novato, USA, Smith, John, Chicago, USA, ...)

    // === Шаг 3: Преобразование RDD (map) ===
    // Преобразуем каждый элемент в пару (элемент, 1).
    // Это типичный шаг для подсчета частоты слов/элементов.
    println("\n=== Шаг 3: RDD после map ((_, 1)) ===")
    val rdd2 = rdd1.map { word => (word, 1) }
    // Альтернативный синтаксис: val rdd2 = rdd1.map(x => (x, 1))
    println(
      s"Данные после map (пары ключ-значение): ${rdd2.collect().mkString(", ")}"
    )
    // Ожидаемый формат: Array[(String, Int)] = Array((Johnson,1), (Rachel,1), (Novato,1), (USA,1), (Smith,1), ...)

    // === Шаг 4: Агрегация по ключу (reduceByKey) ===
    // Суммируем значения для одинаковых ключей.
    // Например, если есть ("USA", 1) и ("USA", 1), результат будет ("USA", 2).
    println("\n=== Шаг 4: RDD после reduceByKey (_ + _) ===")
    val rdd3 = rdd2.reduceByKey { (count1, count2) => count1 + count2 }
    // Альтернативный синтаксис: val rdd3 = rdd2.reduceByKey((x, y) => x + y)
    println(
      s"Данные после reduceByKey (подсчет уникальных элементов): ${rdd3.collect().mkString(", ")}"
    )
    // Ожидаемый результат: Array[(String, Int)] = Array((Vela,1), (Blaise,2), (Ginny,1), (USA,2), ...)

    // === Шаг 5: Сортировка результатов по значению (частоте) в убывающем порядке ===
    // Шаг 5.1: Меняем местами ключ и значение, чтобы значение (частота) стало ключом.
    // Шаг 5.2: Сортируем по новому ключу (бывшему значению) в убывающем порядке (ascending = false).
    // Шаг 5.3: Меняем ключ и значение обратно в исходный формат.
    println("\n=== Шаг 5: RDD после сортировки по значению (частоте) ===")
    val rdd4 = rdd3
      .map { case (word, count) =>
        (count, word)
      } // Меняем (слово, частота) на (частота, слово)
      .sortByKey(ascending =
        false
      ) // Сортируем по частоте (первый элемент пары) в убывающем порядке
      .map { case (count, word) =>
        (word, count)
      } // Меняем обратно на (слово, частота)
    // Альтернативный синтаксис:
    // val rdd4 = rdd3.map(x => x.swap).sortByKey(ascending = false).map(x => x.swap)
    println(
      s"Данные после сортировки по убыванию частоты: ${rdd4.collect().mkString(", ")}"
    )
    // Ожидаемый результат (пример, зависит от данных): Array[(String, Int)] = Array((USA,2), (Blaise,2), (Vela,1), (Ginny,1), ...)

    println("\n--- Практика 2 завершена ---")

    // Останавливаем SparkContext
    sc.stop()
  }
}

// ----------------------- Практика 3: Ключ-значение и группировки -----------------------
object pracThree {

  private val conf = new SparkConf()
    .setAppName("pracThree")
    .setMaster("local[*]")
  private val sc = new SparkContext(conf)

  def preConfig(): Unit = {
    sc.setLogLevel("ERROR")
    println("\n=== Практика 3: Ключ-значение и группировки ===")
  }

  def main(args: Array[String]): Unit = {
    preConfig()

    // Исходный RDD строк (немного расширен для наглядности)
    val lines =
      sc.parallelize(List("this is good good", "this is a test line"), 2)
    // Разбиваем на слова и сразу преобразуем в пары (word, 1)
    val pairs = lines.flatMap(_.split("\\s+")).map(word => (word, 1))
    println(s"Initial 'pairs' RDD (word, 1): ${pairs.collect().mkString(", ")}")

    // 1) groupByKey: сгруппировать по ключу и вывести коллекции значений
    println("\n--- 1) groupByKey ---")
    val gbkCollected = pairs
      .groupByKey()
      .map { case (w, it) => (w, it.toList) }
      .collect()
    println(
      s"1.1) groupByKey on 'pairs' → Array[(String, List[Int])] = ${gbkCollected.mkString(", ")}"
    )

    val dataForGbk2 =
      sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
    val keyedByLength = dataForGbk2.keyBy(_.length) // RDD[(Int, String)]
    val gbk2Collected = keyedByLength
      .groupByKey()
      .map { case (len, words) => (len, words.toSeq) }
      .collect()
    println(
      s"1.2) groupByKey on 'keyedByLength' (length, Seq[String]) → ${gbk2Collected.mkString(", ")}"
    )

    // 2) groupBy: группировка произвольных элементов RDD по функциям
    println("\n--- 2) groupBy ---")
    val nums = sc.parallelize(1 to 9, 3)
    val gbParityCollected = nums
      .groupBy(n => if (n % 2 == 0) "even" else "odd")
      .map { case (k, vs) => (k, vs.toSeq) }
      .collect()
    println(
      s"2.1) groupBy parity on 'nums' → Array[(String, Seq[Int])] = ${gbParityCollected.mkString(", ")}"
    )

    // Пример из материала: groupBy по ключу из RDD пар, затем подсчет размера группы
    val gbKeyCountCollected = pairs // RDD[(String, Int)]
      .groupBy(t =>
        t._1
      ) // Группирует по t._1, результат: RDD[(String, Iterable[(String, Int)])]
      .map { case (key, iter) =>
        (key, iter.size)
      } // Считает количество элементов в каждой группе
      .collect()
    println(
      s"2.2) groupBy key on 'pairs' and count group size → Array[(String, Int)] = ${gbKeyCountCollected
          .mkString(", ")}"
    )
    // Примечание: для подсчета слов reduceByKey обычно эффективнее, чем groupBy + map.

    // 3) reduceByKey: сумма значений по ключу
    println("\n--- 3) reduceByKey ---")
    val reducedArray =
      pairs.reduceByKey(_ + _).collect() // Результат - Array[(String, Int)]
    println(
      s"3) reduceByKey (_+_) on 'pairs' → Array[(String, Int)] = ${reducedArray.mkString(", ")}"
    )

    // 4) aggregateByKey: два примера
    println("\n--- 4) aggregateByKey ---")
    // 4.1) простое суммирование (аналогично reduceByKey для данного случая)
    val agg1Collected = pairs.aggregateByKey(0)(_ + _, _ + _).collect()
    println(
      s"4.1) aggregateByKey(0)(_+_,_+_) on 'pairs' (sum) → ${agg1Collected.mkString(", ")}"
    )

    // 4.2) max на партиции + сумма между партициями
    val dataForAgg2 =
      sc.parallelize(List((1, 3), (1, 2), (1, 4), (2, 3), (3, 6), (3, 8)), 3)
    val agg2Rdd =
      dataForAgg2.aggregateByKey(0)(math.max(_, _), _ + _) // RDD[(Int, Int)]
    val agg2Collected = agg2Rdd.collect()
    println(
      s"4.2) aggregateByKey(0)(math.max(_,_),_+_) on 'dataForAgg2' (3 partitions) → ${agg2Collected.mkString(", ")}"
    )

    // 4.3) Пример с 1 партицией из материала
    val dataForAgg3 =
      sc.parallelize(List((1, 3), (1, 2), (1, 4), (2, 3), (3, 6), (3, 8)), 1)
    val agg3Collected =
      dataForAgg3.aggregateByKey(0)(math.max(_, _), _ + _).collect()
    println(
      s"4.3) aggregateByKey(0)(math.max(_,_),_+_) on 'dataForAgg3' (1 partition) → ${agg3Collected.mkString(", ")}"
    )

    // 5) sortByKey: сортировка RDD[K,V] по ключу K (K должен быть Ordered)
    println("\n--- 5) sortByKey (RDD operation) ---")
    val wordCountsRdd = pairs.reduceByKey(_ + _) // RDD[(String, Int)]

    val sortedByKeyAscCollected =
      wordCountsRdd.sortByKey(ascending = true).collect()
    println(
      s"5.1) sortByKey true (on wordCountsRdd by String key) → ${sortedByKeyAscCollected.mkString(", ")}"
    )

    val sortedByKeyDescCollected =
      wordCountsRdd.sortByKey(ascending = false).collect()
    println(
      s"5.2) sortByKey false (on wordCountsRdd by String key) → ${sortedByKeyDescCollected.mkString(", ")}"
    )

    // Используем agg2Rdd (RDD[(Int, Int)]) из шага 4.2
    val sortedAgg2RddByKeyAscCollected =
      agg2Rdd.sortByKey(ascending = true).collect()
    println(
      s"5.3) sortByKey true (on agg2Rdd by Int key) → ${sortedAgg2RddByKeyAscCollected.mkString(", ")}"
    )

    // 6) sortBy: сортировка RDD[T] по функции f: T => K (K должен быть Ordered)
    println("\n--- 6) sortBy (RDD operation) ---")
    // wordCountsRdd это RDD[(String, Int)]
    // 6.1) Сортировка по значению (количеству) по убыванию
    val sortedByValueDescRddCollected = wordCountsRdd
      .sortBy(
        pair => pair._2,
        ascending = false
      ) // Сортировка по второму элементу пары (значению)
      .collect()
    println(
      s"6.1) RDD sortBy value (desc) on wordCountsRdd → ${sortedByValueDescRddCollected.mkString(", ")}"
    )

    // 6.2) Сортировка по ключу (слову) по убыванию (эквивалентно sortByKey(false) для RDD[K,V])
    val sortedByKeyDescUsingSortByRddCollected = wordCountsRdd
      .sortBy(
        pair => pair._1,
        ascending = false
      ) // Сортировка по первому элементу пары (ключу)
      .collect()
    println(
      s"6.2) RDD sortBy key (desc) on wordCountsRdd → ${sortedByKeyDescUsingSortByRddCollected.mkString(", ")}"
    )

    // 6.3) Пример из материала: sortBy(t => t, false) - сортировка по кортежу целиком
    // Для RDD[(String, Int)], сортирует сначала по String, затем по Int (стандартное сравнение кортежей)
    val sortedByTupleDescRddCollected = wordCountsRdd
      .sortBy(
        tuple => tuple,
        ascending = false
      ) // Сортировка по кортежу (ключ, значение)
      .collect()
    println(
      s"6.3) RDD sortBy tuple (desc) on wordCountsRdd → ${sortedByTupleDescRddCollected.mkString(", ")}"
    )

    // --- Для сравнения: сортировка коллекций Scala (на массивах, не RDD) ---
    println(
      "\n--- Scala Collection Sort (on Arrays, not RDDs for clarification) ---"
    )
    // 'reducedArray' это Array[(String, Int)] из шага 3
    val reducedArraySortedByValueScala =
      reducedArray.sortBy(_._2)(Ordering[Int].reverse)
    println(
      s"Scala: 'reducedArray' sorted by value (desc) → ${reducedArraySortedByValueScala.mkString(", ")}"
    )

    // 'agg2Collected' это Array[(Int, Int)] из шага 4.2
    val agg2CollectedSortedByKeyScala =
      agg2Collected.sortBy(_._1) // по возрастанию ключа
    println(
      s"Scala: 'agg2Collected' sorted by key (asc) → ${agg2CollectedSortedByKeyScala.mkString(", ")}"
    )
    val agg2CollectedSortedByKeyDescScala =
      agg2Collected.sortBy(_._1)(Ordering[Int].reverse) // по убыванию ключа
    println(
      s"Scala: 'agg2Collected' sorted by key (desc) → ${agg2CollectedSortedByKeyDescScala.mkString(", ")}"
    )

    sc.stop()
  }
}

object pracFour {
  private val conf = new SparkConf()
    .setAppName("pracFour")
    .setMaster(
      "local[*]"
    )

  private val sc = new SparkContext(conf)

  def preConfig(): Unit = {
    sc.setLogLevel(
      "ERROR"
    )
    println("\n")
  }

  def main(args: Array[String]): Unit = {
    preConfig()
    println(
      "-------------------- Практика 4: RDD подсчет слов -------------------- "
    )

    val filePath = "/app/src/temperatures.txt"
    val rdd = sc
      .textFile(filePath)
      .filter(_.nonEmpty)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val rdd1 = rdd
      .map(str => {
        val arr = str.split(",")
        val year = arr(0).toInt
        val temp = arr(1).toInt
        (year, (temp, temp))
      })

    rdd1.collect().foreach(print)

    val rdd2 = rdd1
      .reduceByKey { case ((max1, min1), (max2, min2)) =>
        (math.max(max1, max2), math.min(min1, min2))
      }

    rdd2.collect().foreach(println)
  }
}

object pracFive {
  private val spark = SparkSession
    .builder()
    .master("local")
    .appName("pracFive")
    .getOrCreate()

  private val sc = spark.sparkContext

  def preConfig(): Unit = {
    sc.setLogLevel(
      "ERROR"
    )
    println("\n")
  }

  case class Student(
      grade: Int,
      name: String,
      age: Int,
      gender: String,
      subject: String,
      mark: Int
  )

  def main(args: Array[String]): Unit = {
    import spark.implicits._
    preConfig()

    val filePath = "/app/src/people.txt"

    println("1. RDD -> DataFrame")

    val rdd = sc
      .textFile(filePath)
      .filter(_.nonEmpty)

    val header = rdd.first()

    rdd.collect().foreach(println)

    val students = rdd
      .filter(_ != header)
      .map(line => {
        val tokens = line.trim.split("\\s+")
        val grade = tokens(0).toInt
        val name = tokens(1)
        val age = tokens(2).toInt
        val gender = tokens.slice(3, 5).mkString(" ")
        val subject = tokens(5).replaceAll("[,\\.]", "")
        val markStr = tokens(6).replaceAll("[^0-9]", "")
        val mark = if (markStr.nonEmpty) markStr.toInt else 0
        Student(grade, name, age, gender, subject, mark)
      })

    val df = students.toDF()
    df.printSchema()
    df.show()

    println("2. DataFrame -> RDD ")

    val df1 = spark.read.json("/app/src/people.json")
    val rdd1 = df1.rdd
    rdd1.collect().foreach(println)

    println("3. RDD -> Dataset")

    val studentDS = students
      .toDS()

    studentDS.printSchema()
    studentDS.show()

    println("4. Dataset -> RDD")
    val studentRDD = studentDS.rdd
    studentRDD.collect().foreach(println)

    println("5. DataFrame -> Dataset")
    val ds2 = df.as[Student]
    ds2.show()

    println("6. Dataset -> DataFrame")
    val df3 = ds2.toDF()
    df3.show()

    spark.stop()
  }
}
