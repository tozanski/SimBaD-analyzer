package analyzer

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}
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
        col("_2.mutationId").as("parentMutationId").as(Encoders.LONG)
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

  def writeLineage(spark: SparkSession,
              pathPrefix: String,
              mutations: Dataset[MutationTreeLink],
              root: Long = 1): Dataset[Ancestry] = {
    import spark.implicits._

    val lineagesPath = pathPrefix + "/lineages.parquet"

    // clear previous results
    spark.sparkContext.setJobGroup("init tmp lineages", "empty lineage")
    spark.
      emptyDataset[Ancestry].
      write.
      mode("overwrite").
      parquet(lineagesPath)

    var selectedBuffer1 = "selected_buf_1"
    var selectedBuffer2 = "selected_buf_2"

    //var selectedTmpPath: String = pathPrefix + "/selected1.parquet"
    //var selectedTmpPathOther: String = pathPrefix + "/selected2.parquet"

    spark.sparkContext.setJobGroup("init selected", "first selected node")
    Seq((root, Array[Long](root))).
      toDF("mutationId", "ancestors").
      as[Ancestry].
      write.
      mode("overwrite").
      saveAsTable(selectedBuffer1)

    //val all_mutations: Dataset[MutationTreeLink] =
    mutations.
      write.
      bucketBy(100, "parentMutationId").
      sortBy("parentMutationId").
      saveAsTable("mutation_tree")
    val all_mutations = spark.table("mutation_tree").
      as[MutationTreeLink]

    spark.sparkContext.setJobGroup("all mutation count", "debug all mutation count")
    val all_count: Long = all_mutations.count
    println(s"all_count =  $all_count")

    var i: Long = 0
    var complete_sum: Long = 0

    while(complete_sum < all_count){
      println(s"iteration = $i")
      i+=1

      val selected: Dataset[Ancestry] = spark.table(selectedBuffer1).as[Ancestry]

      spark.sparkContext.setJobGroup("count selected", "count selected")
      val selected_count: Long = selected.count
      println(s"selected.count = $selected_count")

      if(0 == selected_count)
        throw new RuntimeException("something bad with the tree...")

      spark.sparkContext.setJobGroup("append selected", "append lineage data")
      selected.
        write.
        //sortBy("mutationId").
        //bucketBy(1024, "mutationId").
        mode(SaveMode.Append).
        parquet(lineagesPath)

      complete_sum += selected_count
      println(s"complete_sum = $complete_sum")

      spark.sparkContext.setJobGroup("new selected", "BFS new selected tree nodes")
      all_mutations.
        joinWith(selected, all_mutations.col("parentMutationId")===selected.col("mutationId") ).
        map( x => Ancestry(x._1.mutationId, x._2.ancestors :+ x._1.mutationId)).
        write.
        mode("overwrite").
        saveAsTable(selectedBuffer2)

      val tmpBuffer = selectedBuffer1
      selectedBuffer1 = selectedBuffer2
      selectedBuffer2 = tmpBuffer
    }

    spark.
      read.
      parquet(lineagesPath).
      as[Ancestry]
  }
  def getOrComputeMutationTree(spark: SparkSession, pathPrefix: String, chronicles: Dataset[ChronicleEntry]): Dataset[MutationTreeLink] = {
    import spark.implicits._

    val mutationPath = pathPrefix + "/mutation_tree.parquet"
    try{
      spark.read.parquet(mutationPath).as[MutationTreeLink]
    }catch {
      case _: Exception =>
        spark.sparkContext.setJobGroup("mutation tree", "save mutation tree")
        mutationTree(chronicles).write.mode("overwrite").parquet(mutationPath)
        spark.read.parquet(mutationPath).as[MutationTreeLink]
    }
  }

  def getOrComputeLineages(spark: SparkSession, pathPrefix: String, mutationTree: Dataset[MutationTreeLink]): Dataset[Ancestry] = {
    import spark.implicits._

    val lineagesPath = pathPrefix + "/lineages.parquet"
    try{
      spark.read.parquet(lineagesPath).as[Ancestry]
    }catch{
      case _: Exception =>
        spark.sparkContext.setJobGroup("lineage","phylogeny lineage")
        writeLineage(spark, pathPrefix, mutationTree)

        spark.read.parquet(lineagesPath).as[Ancestry]
      }
    }

  def main(args: Array[String]) = {
    if( args.length != 1 )
      throw new RuntimeException("no prefix path given")

    val pathPrefix = args(0)

    val spark = SparkSession.builder.
      appName("Phylogeny Testing").
      getOrCreate()
    spark.sparkContext.setCheckpointDir(pathPrefix + "/tmp")

    val chronicles = Chronicler.computeOrReadChronicles(spark, pathPrefix)

    val maxTime = Analyzer.getMaxTime(chronicles)

    val mutationTree = Phylogeny.getOrComputeMutationTree(spark, pathPrefix, chronicles)

    val lineages = writeLineage(spark, pathPrefix, mutationTree)//getOrComputeLineages(spark, pathPrefix, mutationTree)

   /* spark.sparkContext.setJobGroup("muller","compute & save muller plot data")
    Analyzer.
      saveCSV(pathPrefix + "/muller_plot_data",
      Muller.mullerData(spark, chronicles, lineages, maxTime, 1000).toDF,
      true)

    */
  }
}
