import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object dataFramesExamplesLoadingFile  extends App {//extends App is important
val sparkConf = new SparkConf()
sparkConf.set("spark.app.name", "my first application")
sparkConf.set("spark.master","local[2]")

val spark = SparkSession.builder()	
.config(sparkConf)			//.appName("MY application 1") this lines are skiped
.getOrCreate()				//.master("local[2]")


val ordersDf = spark.read
.option("header",true)
.option("inferSchema",true)
.csv("D:/week11/orders.csv")
 
ordersDf.show()

scala.io.StdIn.readLine()
spark.stop()
}