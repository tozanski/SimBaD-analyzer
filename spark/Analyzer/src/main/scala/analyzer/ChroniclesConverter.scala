package analyzer

import org.apache.spark.sql.SparkSession

object ChroniclesConverter {

  def main(args: Array[String]) {

    if( args.length != 1 )
      throw new RuntimeException("no prefix path given");

    args.foreach( println );
    val pathPrefix = args(0);

    val spark = SparkSession.builder.
      appName("SimBaD analyzer").
      getOrCreate()

    ChronicleLoader.loadEntries( spark, pathPrefix + "/chronicles.csv.gz" ).
      write.
      format("parquet").
      save(pathPrefix+"/chronicles.parquet")
  }
}
