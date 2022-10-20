import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object dataFramesExample1 extends App{
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first application")
  sparkConf.set("spark.master","local[2]")

  val spark = SparkSession.builder()	
  .config(sparkConf)			//.appName("MY application 1") this lines are skiped
  .getOrCreate()				//.master("local[2]")

spark.stop()
}
