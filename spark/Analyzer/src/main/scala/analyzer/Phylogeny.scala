package analyzer

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.storage.StorageLevel

object Phylogeny  {
  def mutationTree(chronicles: Dataset[ChronicleEntry]): Dataset[MutationTreeLink] = {

    val children = chronicles.
      select(
        col("parentId").as(Encoders.LONG),
        col("mutationId").as(Encoders.LONG)).
      alias("children")

    val parents = chronicles.
      select(
        col("particleId").as(Encoders.LONG),
        col("mutationId").as(Encoders.LONG)).
      alias("parents")

    children.
      joinWith(parents, col("children.parentId")===col("parents.particleId"), "left_outer").
      filter(col("_1.mutationId") =!= col("_2.mutationId")).
      select(
        col("_1.mutationId").as("mutationId").as(Encoders.LONG),
        col("_2.mutationId").as("parentId").as(Encoders.LONG)
      ).
      as(Encoders.product[MutationTreeLink])
  }

/*
    val pathPrefix = "/scratch/WCSS/20190110-202151-334492354-simulation-test/"

    case class MutationTreeLink(mutationId: Long, parentId: Long)
    case class Ancestry(mutationId: Long, ancestors: Array[Long])

    val mutations = spark.
      read.parquet(pathPrefix + "/mutationTree.parquet").
      select("id", "parentId").
      as[(Long,Long)].
      withColumnRenamed("id","mutationId").
      as[MutationTreeLink]
*/

  def lineage(spark: SparkSession,
              pathPrefix: String,
              mutations: Dataset[MutationTreeLink],
              root: Long = 1): Dataset[Ancestry] = {
    import spark.implicits._

    val lineagesTmpPath = pathPrefix + "/lineages.tmp"

    // clear previous results
    spark.
      emptyDataset[Ancestry].
      write.
      mode("overwrite").
      parquet(lineagesTmpPath)

    var selectedTmpPath: String = pathPrefix + "/selected1"
    var selectedTmpPathOther: String = pathPrefix + "/selected2"

    Seq((root, Array[Long](root))).
      toDF("mutationId", "ancestors").
      as[Ancestry].
      write.
      mode("overwrite").
      parquet(selectedTmpPath)

    val all_mutations: Dataset[MutationTreeLink] = mutations.
      sort("parentId").
      as[MutationTreeLink].
      persist()

    val all_count: Long = all_mutations.count
    println(s"all_count =  $all_count")

    var i: Long = 0
    var complete_sum: Long = 0

    while(complete_sum < all_count){
      println(s"iteration = $i")
      i+=1

      val selected: Dataset[Ancestry] = spark.
        read.parquet(selectedTmpPath).
        as[Ancestry]

      if(selected.isEmpty)
        throw new RuntimeException("something bad with the tree...")

      selected.
        write.
        //sortBy("mutationId").
        //bucketBy(1024, "mutationId").
        mode("append").
        parquet(lineagesTmpPath)

      val selected_count: Long = selected.count
      println(s"selected.count = $selected_count")
      complete_sum += selected_count
      println(s"complete_sum = $complete_sum")

      all_mutations.
        joinWith(selected, all_mutations.col("parentId")===selected.col("mutationId") ).
        map( x => Ancestry(x._1.mutationId, x._2.ancestors :+ x._1.mutationId)).
        write.
        mode("overwrite").
        parquet(selectedTmpPathOther)

      val tmpPath = selectedTmpPathOther
      selectedTmpPathOther = selectedTmpPath
      selectedTmpPath = tmpPath
    }

    return spark.
      read.
      parquet(lineagesTmpPath).
      as[Ancestry]
  }
  def getOrComputeMutationTree(spark: SparkSession, pathPrefix: String, chronicles: Dataset[ChronicleEntry]): Dataset[MutationTreeLink] = {
    import spark.implicits._

    var mutations: Dataset[MutationTreeLink] = null
    val mutationPath = pathPrefix + "/mutationTree.parquet"
    try{
      mutations = spark.read.parquet(mutationPath).as[MutationTreeLink]
    }catch{
      case e: Exception => {
        spark.sparkContext.setJobGroup("mutation tree", "save mutation tree")
        mutationTree(chronicles).write.mode("overwrite").parquet(mutationPath)
        mutations = spark.read.parquet(mutationPath).as[MutationTreeLink]
      }
    }
    return mutations
  }

  def getOrComputeLineages(spark: SparkSession, pathPrefix: String, mutationTree: Dataset[MutationTreeLink]): Dataset[Ancestry] = {
    import spark.implicits._

    var lineages: Dataset[Ancestry] = null
    val lineagesPath = pathPrefix + "/lineages.parquet"
    try{
      lineages = spark.read.parquet(lineagesPath).as[Ancestry]
    }catch{
      case e: Exception => {
        spark.sparkContext.setJobGroup("lineage","phylogeny lineage")
        lineage(spark, pathPrefix, mutationTree).
          repartition($"mutationId").
          write.
          mode("overwrite").
          parquet(lineagesPath)
        lineages = spark.read.parquet(lineagesPath).as[Ancestry]
      }
    }
    return lineages
  }
/*
  def main_(args: Array[String]) = {
    if( args.length != 1 )
      throw new RuntimeException("no prefix path given");

    val pathPrefix = args(0);

    val spark = SparkSession.builder.
      appName("Phylogeny Testing").
      getOrCreate()
    spark.sparkContext.setCheckpointDir(pathPrefix + "/tmp")

    val chronicles = ChronicleLoader.getOrConvertChronicles(spark, pathPrefix)
    spark.sparkContext.setJobGroup("max Time", "computing maximum time")
    val maxTime = Analyzer.getMaxTime(chronicles);

    val mutationTree = Phylogeny.getOrComputeMutationTree(spark, pathPrefix, chronicles)

    val lineages = getOrComputeLineages(spark, pathPrefix, mutationTree)

    spark.sparkContext.setJobGroup("muller","compute & save muller plot data")
    Analyzer.saveCSV(pathPrefix + "/muller_plot_data",
      Muller.mullerData(spark, chronicles, lineages, maxTime, 1000).toDF,
      true);
  }*/
}
