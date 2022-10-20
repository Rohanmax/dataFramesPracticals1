import scala.io.Source
import org.apache.log4j
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark
import org.apache.spark.SparkContext
import scala.io.BufferedSource

object topMovieRatedWithBroadcastJoin extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","topMovies")
  
  val moviesRdd = sc.textFile("D:/week11/movies.dat")
 
  val boradcastRdd = sc.broadcast(moviesRdd.map(x => (x.split("::")(0),x.split("::")(1))).collect().toMap)
  
  val ratingsInput = sc.textFile("D:/week11/ratings.dat")
  
  val mappedRdd = ratingsInput.map(x => {
   val fields = x.split("::")
   (fields(1),fields(2))
  })
  
  val newMapped = mappedRdd.mapValues(x => (x.toFloat,1.0))
  
  val reducedRdd = newMapped.reduceByKey((x,y) => (x._1 + y._1 , x._2 + y._2))
  
  val filteredRdd = reducedRdd.filter(x => x._2._2 > 10)
  
  val ratingsProcessed = filteredRdd.mapValues(x => x._1/x._2).filter(x => x._2 > 4.0)
  
  val finalData = ratingsProcessed.map(x => (x._1,x._2, boradcastRdd.value(x._1)))
  
  //finalData.collect().foreach(println)
  
}