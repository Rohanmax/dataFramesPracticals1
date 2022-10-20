import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object dataFrameReaderApi  extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","my first application")
  sparkConf.set("spark.master","local[2]")


 val spark = SparkSession. builder()
    .config(sparkConf)			
    .getOrCreate()			

   /*
   val ordersDf = spark.read
	.format("csv")
	.option("header",true)
	.option("inferSchema",true)
	.option("path","D:/week11/orders.csv")
	.load
	*/
    
/*
 val ordersDf = spark.read
	.format("json")
	.option("path","D:/week11/players.json")
  .option("mode","DROPMALFORMED")
	.load
*/
  
 val ordersDf = spark.read
	.option("path","D:/week11/users.parquet")
	.load
	
 ordersDf.printSchema

 ordersDf.show(false)
 
scala.io.StdIn.readLine()
spark.stop()
  
}