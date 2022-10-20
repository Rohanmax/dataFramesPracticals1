import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.log4j
import scala.math.min
import org.apache.log4j.Logger
import org.apache.log4j.Level

object topMovieRated extends App {
  
Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","topMovies")
  
  val input = sc.textFile("D:/week11/ratings.dat")
  
  val mappedRdd = input.map(x => {
   val fields = x.split("::")
   (fields(1),fields(2))
  })
  
  val newMapped = mappedRdd.mapValues(x => (x.toFloat,1.0))
  
  val reducedRdd = newMapped.reduceByKey((x,y) => (x._1 + y._1 , x._2 + y._2))
  
  val filteredRdd = reducedRdd.filter(x => x._2._2 > 10)
  
  val ratingsProcessed = filteredRdd.mapValues(x => x._1/x._2).filter(x => x._2 > 4.0)
  
  //ratingsProcessed.collect.foreach(println)
  
 val moviesRdd = sc.textFile("D:/week11/movies.dat")
 
 val moviesMapped = moviesRdd.map(x => {
   val fields = x.split("::")
   (fields(0),fields(1))
 })
 
val joinedMovies =  moviesMapped.join(ratingsProcessed)

val finalResult = joinedMovies.map(x => x._2._1)

finalResult.collect.foreach(println)



scala.io.StdIn.readLine()
}