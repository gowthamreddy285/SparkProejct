
import org.apache.spark.sql.SparkSession

object RetailIQ_Spark {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("RetailIQ_FreshImplementation")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    val sc = spark.sparkContext

    val datasetPath =
      "file:///home/satwiqreddy/SparkProject/data/events.csv"

    // =====================================================
    // TASK 1: RDD CREATION
    // =====================================================

    println("\n========== TASK 1: RDD CREATION ==========")

    val rawRDD = sc.textFile(datasetPath)

    println("RDD created successfully")
    println("Total lines including header: " + rawRDD.count())
    println("CSV header: " + rawRDD.first())
    println("Number of input partitions: " + rawRDD.getNumPartitions)

    val header = rawRDD.first()
    val dataRDD = rawRDD.filter(line => line != header)

    println("Data records excluding header: " + dataRDD.count())
    println("Sample data record: " + dataRDD.take(2).last)

    // =====================================================
    // TASK 2: TRANSFORMATIONS
    // =====================================================

    println("\n========== TASK 2: TRANSFORMATIONS ==========")

    val validLinesRDD = dataRDD.filter(_.trim.nonEmpty)

    println("Transformation 1 - filter()")
    println("Non-empty records: " + validLinesRDD.count())

    val parsedRDD = validLinesRDD.map(line => line.split(",", -1))

    println("Transformation 2 - map()")
    println(
      "Parsed record example: " +
      parsedRDD.take(1).head.mkString(" | ")
    )

    val eventTypesRDD = validLinesRDD.flatMap { line =>
      val fields = line.split(",", -1)

      if (fields.length >= 3 && fields(2).nonEmpty)
        Seq(fields(2))
      else
        Seq.empty[String]
    }

    println("Transformation 3 - flatMap()")
    println("Event type sample: " + eventTypesRDD.take(5).mkString(", "))

    val uniqueEventTypesRDD = eventTypesRDD.distinct()

    println("Transformation 4 - distinct()")
    println(
      "Unique event types: " +
      uniqueEventTypesRDD.collect().mkString(", ")
    )

    val eventCountsRDD = eventTypesRDD
      .map(eventType => (eventType, 1))
      .reduceByKey(_ + _)

    println("Transformation 5 - reduceByKey()")
    println("Event counts by type:")

    eventCountsRDD.collect().foreach(println)

    // =====================================================
    // TASK 3: ACTIONS
    // =====================================================

    println("\n========== TASK 3: ACTIONS ==========")

    println("Action 1 - collect():")
    eventCountsRDD.collect().foreach(println)

    val totalRecords = validLinesRDD.count()
    println("Action 2 - count(): " + totalRecords)

    println("Action 3 - take():")
    validLinesRDD.take(3).foreach(println)

    val totalEvents = eventCountsRDD
      .map { case (_, count) => count }
      .reduce(_ + _)

    println("Action 4 - reduce(): " + totalEvents)

    // =====================================================
    // TASK 4: KEY-VALUE RDD
    // =====================================================

    println("\n========== TASK 4: KEY-VALUE RDD ==========")

    println("Event type counts:")
    eventCountsRDD.collect().foreach(println)

    val visitorEventCountsRDD = validLinesRDD
      .map { line =>
        val fields = line.split(",", -1)
        (fields(1).toInt, 1)
      }
      .reduceByKey(_ + _)

    println(
      "Number of unique visitors: " +
      visitorEventCountsRDD.count()
    )

    println("Sample visitor event counts:")
    visitorEventCountsRDD.take(5).foreach(println)

    // =====================================================
    // TASK 5: PARTITIONS
    // =====================================================

    println("\n========== TASK 5: PARTITIONS ==========")

    println("Raw RDD partitions: " + rawRDD.getNumPartitions)
    println(
      "Valid records RDD partitions: " +
      validLinesRDD.getNumPartitions
    )
    println(
      "Visitor-count RDD partitions: " +
      visitorEventCountsRDD.getNumPartitions
    )

    // =====================================================
    // TASK 6: RDD LINEAGE
    // =====================================================

    println("\n========== TASK 6: RDD LINEAGE ==========")

    println("\nLineage of eventCountsRDD:")
    println(eventCountsRDD.toDebugString)

    println("\nLineage of visitorEventCountsRDD:")
    println(visitorEventCountsRDD.toDebugString)

    println(
      "\nExplanation: RDD lineage records the transformations " +
      "used to produce an RDD. Spark can use this information " +
      "to recompute lost partitions when possible."
    )

    // =====================================================
    // TASK 7: DAG REPRESENTATION
    // =====================================================

    println("\n========== TASK 7: DAG REPRESENTATION ==========")

    println(
      """
      Event-count processing DAG:

      Input CSV
          |
          v
      textFile() -> rawRDD
          |
          v
      filter() -> dataRDD
          |
          v
      filter() -> validLinesRDD
          |
          v
      flatMap() -> eventTypesRDD
          |
          v
      map(eventType -> (eventType, 1))
          |
          v
      reduceByKey(_ + _)
          |
          v
      eventCountsRDD
          |
          v
      collect() / output

      Note:
      reduceByKey() requires data to be grouped by key and
      normally introduces a shuffle between partitions.
      Spark builds its execution DAG from RDD dependencies
      and splits execution into stages around shuffle boundaries.
      """
    )

    println("RDD lineage is printed in Task 6.")
    println(
      "For the actual execution DAG, use the Spark Web UI " +
      "while the application is running."
    )

    spark.stop()
  }
}
