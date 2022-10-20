import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.arrow.flatbuf.Timestamp

object dataFrameExplicitSchema  extends App {

  case class OrdersData (order_id: Int, order_date: Timestamp, order_customer_id: Int, order_status: String)
    
    val sparkConf = new SparkConf()
    sparkConf.set("spark.app.name","my first application")
    sparkConf.set("spark.master","local[2]")

    val spark = SparkSession. builder()
    .config(sparkConf)			
    .getOrCreate()			
    
//Programatic approach 
/*
  val ordersSchema = StructType(List(
	StructField("orderid",IntegerType),
	StructField("orderdate",TimestampType),
	StructField("customerid",IntegerType ),
	StructField("status", StringType)
	))

	   val ordersDf = spark.read
	  .format("csv")
  	.option("header",true)
  	.schema(ordersSchema)
  	.option("path","D:/week11/orders.csv")
	.load
    */
    
 //DDL String Approach   
  val ordersSchemaDDL = "orderid Int, orderdate String, custid  Int, orderstatus String"
    
    val ordersDf = spark.read
	  .format("csv")
  	.option("header",true)
  	.schema(ordersSchemaDDL)
  	.option("path","D:/week11/orders.csv")
	.load

	import spark.implicits._
  
	//val ordersDs = ordersDf.as[OrdersData]
	
	ordersDf.printSchema()

	ordersDf.show()
   scala.io.StdIn.readLine()
    spark.stop()
  
}