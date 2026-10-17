package tasks

import org.apache.spark.sql.SparkSession

// DAG (Directed Acyclic Graph)
object DAG {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DAG")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val dataPath = "file:///Users/kireeti/Desktop/Projects/SparkProejct/data/events.csv"

    println("\nTASK 7: DAG (DIRECTED ACYCLIC GRAPH)")

    println("\nWhat is DAG?")
    println("- Visual representation of RDD transformations")
    println("- Shows dependencies between RDDs")
    println("- Directed: flows in one direction")
    println("- Acyclic: no loops")

    println("\nOur processing DAG:")
    println("""
    events.csv
        |
        v
    textFile() ----→ rawRDD
        |
        v
    filter() ------→ dataRDD (remove header)
        |
        v
    flatMap() -----→ eventsRDD (extract event column)
        |
        v
    map() ---------→ pairs RDD (event, 1)
        |
        v
    reduceByKey() -→ eventCounts (sum by event type)
        |
        v
    collect() -----→ Results
    """)

    println("\nStages in DAG:")
    println("- Stage 1: Read and transform (no shuffle)")
    println("- Stage 2: reduceByKey (requires shuffle)")
    println("- Shuffle = data exchange between partitions")

    println("\nDAG EXPLANATION COMPLETE\n")

    spark.stop()
  }
}
