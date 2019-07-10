package analyzer

import org.apache.spark.sql.functions.{col, explode, first, isnull, lit, greatest, struct, sum}
import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

object Phylogeny  {
  def mutationBranches(chronicles: Dataset[ChronicleEntry]): Dataset[Mutation] = {

    val children = chronicles.
      select(
        col("birthTime").alias("time").as(Encoders.scalaDouble),
        col("parentId").as(Encoders.scalaLong),
        col("mutationId").as(Encoders.scalaLong),
        col("cellParams").as(Encoders.product[CellParams])
      )

    val parents = chronicles.
      select(
        col("particleId").alias("parentId").as(Encoders.scalaLong),
        col("mutationId").alias("parentMutationid").as(Encoders.scalaLong)
      )

    children.
      join(parents, Seq("parentId"), "left").
      na.fill(0).
      filter(col("mutationId") =!= col("parentMutationId")).
      repartitionByRange(col("mutationId")).
      dropDuplicates("mutationId").
      select(
        col("mutationId").as(Encoders.scalaLong),
        col("cellParams").as(Encoders.product[CellParams]),
        col("parentMutationId").as(Encoders.scalaLong),
        col("time").as(Encoders.scalaDouble)
      ).
      as(Encoders.product[Mutation])
  }

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

    spark.sparkContext.setJobGroup("init selected", "first selected node")
    Seq((root, Array[Long](root))).
      toDF("mutationId", "ancestors").
      as[Ancestry].
      write.
      mode("overwrite").
      saveAsTable(selectedBuffer1)

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

  def mutationCounts(lineages: Dataset[Ancestry], clones: Dataset[Clone], mutations: Dataset[Mutation]): Dataset[MutationSummary] = {
    val accumulatedCounts = clones.
      join(lineages, Seq("mutationId"), "right" ).
      na.fill(0).
      withColumn("ancestorId", explode(col("ancestors"))).
      groupBy("ancestorId").
      agg(
        sum("count").as("mutationCount")
      ).
      withColumnRenamed("ancestorId", "mutationId")

    val parentMutations = mutations.
      select(
        col("mutationId").alias("parentMutationId"),
        col("cellParams").alias("parentParams"),
        col("time").alias("parentTime")
      ).alias("parent_mutations")

    val mutationParams = mutations.
      join(parentMutations, Seq("parentMutationId"), "left").
      na.fill(0).
      select(
        col("mutationId"),
        col("cellParams"),
        col("time"),
        (col("time") - greatest(lit(0),col("parentTIme"))).alias("waitingTime"),
        struct(
          (col("cellParams.birthEfficiency")-col("parentParams.birthEfficiency")).alias("birthEfficiency"),
          (col("cellParams.birthResistance")-col("parentParams.birthResistance")).alias("birthResistance"),
          (col("cellParams.lifespanEfficiency")-col("parentParams.lifespanEfficiency")).alias("lifespanEfficiency"),
          (col("cellParams.lifespanResistance")-col("parentParams.lifespanResistance")).alias("lifespanResistance"),
          (col("cellParams.successEfficiency")-col("parentParams.successEfficiency")).alias("successEfficiency"),
          (col("cellParams.successResistance")-col("parentParams.successResistance")).alias("successResistance")
        ).alias("parameterUpgrades")
      )

    clones.
      select(col("mutationId"), col("count").alias("typeCount")).
      join(accumulatedCounts, Seq("mutationId"), "right").
      na.fill(0).
      join(lineages, Seq("mutationId")).
      join(
        mutationParams,
        Seq("mutationId")
      ).
      as(Encoders.product[MutationSummary])
  }

  def getOrComputeMutationBranches(pathPrefix: String, chronicles: Dataset[ChronicleEntry]): Dataset[Mutation] = {
    val spark = chronicles.sparkSession
    import spark.implicits._

    val mutationPath = pathPrefix + "/mutation_tree.parquet"
    try{
      spark.read.parquet(mutationPath).as[Mutation]
    }catch {
      case _: Exception =>
        spark.sparkContext.setJobGroup("mutation tree", "save mutation tree")
        mutationBranches(chronicles).write.mode("overwrite").parquet(mutationPath)
        spark.read.parquet(mutationPath).as[Mutation]
    }
  }

  def getOrComputeLineages(pathPrefix: String, mutationTree: Dataset[MutationTreeLink]): Dataset[Ancestry] = {
    val spark = mutationTree.sparkSession
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

  def getOrComputeMutationCounts(path: String,
                                      lineages: Dataset[Ancestry],
                                      clones: Dataset[Clone],
                                      mutations: Dataset[Mutation]): Dataset[MutationSummary] ={
    val spark = clones.sparkSession
    import spark.implicits._

    try{
      spark.read.parquet(path).as[MutationSummary]
    }catch{
      case _: Exception =>
        spark.sparkContext.setJobGroup("clone counts", "compute clone counts")
        mutationCounts(lineages, clones, mutations).write.mode(SaveMode.Overwrite).parquet(path)
        spark.read.parquet(path).as[MutationSummary]
    }
  }
}
