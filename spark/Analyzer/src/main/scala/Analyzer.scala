import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Analyzer {
  def main(args: Array[String]) {
    
    if( args.length != 1 )
      throw new RuntimeException("no prefix path given")
    
    args.foreach( println )
    val prefixPath = args(0)
    //val prefixPath = "/Users/tomek/SimBaD/run/"
    
    val chroniclesPath = prefixPath + "/chronicles.csv.gz"
    val conf = new SparkConf().setAppName("SimBaD analyzer")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(chroniclesPath, 4).cache()
    val numLines = textFile.count()

    println("There are %s lines".format(numLines))
  }
}