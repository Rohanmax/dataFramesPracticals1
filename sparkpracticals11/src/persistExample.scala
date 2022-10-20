import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel


object persistExample extends App {

Logger.getLogger("org").setLevel(Level.ERROR)
  
val sc = new SparkContext("local[*]","wordcount")

val input = sc.textFile("D:/weeknine/customerorders.csv")

val mappedInput = input.map(x => (x.split(",")(0),x.split(",")(2).toFloat))

val totalByCustomer = mappedInput.reduceByKey((x,y) => x+y)

val premiumCustomers = totalByCustomer.filter(x => x._2 > 5000)

val doubledAmount = premiumCustomers.map(x => (x._1, x._2 * 2)).persist(StorageLevel.MEMORY_AND_DISK)

doubledAmount.collect.foreach(println)

println(doubledAmount.count)

scala.io.StdIn.readLine()
}