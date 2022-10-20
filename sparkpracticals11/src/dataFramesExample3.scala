import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

object dataFramesExample3  extends App {
  
 Logger.getLogger("org").setLevel(Level.ERROR)
  
 val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","my first application")
  sparkConf.set("spark.master","local[2]")


  val spark = SparkSession. builder()	
    .config(sparkConf)			
    .getOrCreate()			

    val ordersDf = spark.read
	  .option("header",true)
	  .option("inferSchema",true)
	  .csv("D:/week11/orders.csv")

val groupedOrdersDf = ordersDf
.repartition(4)
.where("order_customer_id > 10000")
.select("order_id","order_customer_id")
.groupBy("order_customer_id")
.count()

groupedOrdersDf.show()

 scala.io.StdIn.readLine()
 spark.stop()
  
}