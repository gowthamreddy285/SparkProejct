package tasks

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

// Cache and Persist
object CachePersist {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("CachePersist")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val dataPath = "file:///Users/kireeti/Desktop/Projects/SparkProejct/data/events.csv"

    println("\nTASK 8: CACHE AND PERSIST")

    // Load data
    val rawRDD = sc.textFile(dataPath)
    val header = rawRDD.first()
    val dataRDD = rawRDD.filter(line => line != header)

    // Demo 1: cache()
    println("\n1. Demonstrating cache()")
    val cachedRDD = dataRDD.cache()

    println("   First count (computes and caches)...")
    val start1 = System.currentTimeMillis()
    val count1 = cachedRDD.count()
    val time1 = System.currentTimeMillis() - start1
    println(f"   Result: $count1%,d records in ${time1}ms")

    println("\n   Second count (uses cache)...")
    val start2 = System.currentTimeMillis()
    val count2 = cachedRDD.count()
    val time2 = System.currentTimeMillis() - start2
    println(f"   Result: $count2%,d records in ${time2}ms")
    println(s"   Speedup: ${(time1.toDouble/time2).formatted("%.1f")}x faster!")

    // Demo 2: persist()
    println("\n2. Demonstrating persist()")
    val persistedRDD = dataRDD.persist(StorageLevel.MEMORY_AND_DISK)
    println("   Storage: MEMORY_AND_DISK")
    println("   (Uses memory first, spills to disk if needed)")

    val count3 = persistedRDD.count()
    println(f"   Persisted $count3%,d records")

    // Cleanup
    cachedRDD.unpersist()
    persistedRDD.unpersist()
    println("\n3. Called unpersist() to free memory")

    println("\nWhen to use cache/persist:")
    println("- When RDD is used multiple times")
    println("- Iterative algorithms (machine learning)")
    println("- Interactive analysis")
    println("- Speeds up repeated access")

    println("\nCACHE/PERSIST DEMO COMPLETE\n")

    spark.stop()
  }
}
