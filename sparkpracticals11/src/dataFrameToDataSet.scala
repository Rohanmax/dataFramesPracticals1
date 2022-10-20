import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset
import org.apache.arrow.flatbuf.Timestamp
import org.apache.spark.sql.Row




object dataFrameToDataSet  extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  
case class OrdersData (order_id: Int, order_date: Timestamp, order_customer_id: Int, order_status: String)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","my first application")
  sparkConf.set("spark.master","local[2]")


  val spark = SparkSession. builder()	
    .config(sparkConf)			
    .getOrCreate()			

  val ordersDf: Dataset[Row] = spark.read
	.option("header",true)
	.option("inferSchema",true)
	.csv("D:/week11/orders.csv")

    
  import spark.implicits._
  
	val ordersDs = ordersDf.as[OrdersData]
  
  ordersDs.filter(x=> x.order_id <10 )
  
  ordersDs.show()
  
   scala.io.StdIn.readLine()
  spark.stop()
  
}