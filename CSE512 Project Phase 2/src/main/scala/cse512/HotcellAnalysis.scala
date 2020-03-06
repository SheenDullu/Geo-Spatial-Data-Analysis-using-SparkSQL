package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
 Logger.getLogger("org.spark_project").setLevel(Level.WARN)
 Logger.getLogger("org.apache").setLevel(Level.WARN)
 Logger.getLogger("akka").setLevel(Level.WARN)
 Logger.getLogger("com").setLevel(Level.WARN)

 def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {
 // Load the original data from a data source
 var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter", ";").option("header", "false").load(pointPath);
 pickupInfo.createOrReplaceTempView("nyctaxitrips")
 pickupInfo.show()

 // Assign cell coordinates based on pickup points
 spark.udf.register("CalculateX", (pickupPoint: String) => ((
 HotcellUtils.CalculateCoordinate(pickupPoint, 0)
 )))
 spark.udf.register("CalculateY", (pickupPoint: String) => ((
 HotcellUtils.CalculateCoordinate(pickupPoint, 1)
 )))
 spark.udf.register("CalculateZ", (pickupTime: String) => ((
 HotcellUtils.CalculateCoordinate(pickupTime, 2)
 )))
 pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
 var newCoordinateName = Seq("x", "y", "z")
 pickupInfo = pickupInfo.toDF(newCoordinateName: _*)
 pickupInfo.createOrReplaceTempView("pickupInfo")

 // Define the min and max of x, y, z
 val minX = -74.50 / HotcellUtils.coordinateStep
 val maxX = -73.70 / HotcellUtils.coordinateStep
 val minY = 40.50 / HotcellUtils.coordinateStep
 val maxY = 40.90 / HotcellUtils.coordinateStep
 val minZ = 1
 val maxZ = 31
 val numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)

 // YOU NEED TO CHANGE THIS PART
 val cellPoints = spark.sql("select x,y,z from pickupInfo " +
 " where x >= " + minX + " and x <= " + maxX +
 " and y >= " + minY + " and y <= " + maxY +
 " and z >= " + minZ + " and z <= " + maxZ +
 " order by z, y, x desc")
 cellPoints.createOrReplaceTempView("cellPoints")

 val cellHotnessInfo = spark.sql("select x, y, z, count(*) as countHotCells from cellPoints group by x,y,z order by z,y,x desc")
 cellHotnessInfo.createOrReplaceTempView("cellHotnessInfo")

 val sumOfSelectedPoints = spark.sql("select sum(countHotCells) as sumHotCellPoints from cellHotnessInfo");
 sumOfSelectedPoints.createOrReplaceTempView("sumOfSelectedPoints")

 val sumHotCellPoints = sumOfSelectedPoints.first().getLong(0).toDouble
 val mean = (sumHotCellPoints / numCells.toDouble).toDouble

 spark.udf.register("squared", (inputX: Int) => HotcellUtils.square(inputX))
 val sumOfSquaredCells = spark.sql("select sum(squared(countHotCells)) as sumOfSquaredCell from cellHotnessInfo")
 sumOfSquaredCells.createOrReplaceTempView("sumOfSquaredCells")


 val sumOfSquaredCellValue = sumOfSquaredCells.first().getDouble(0)
 val standardDeviation = Math.sqrt((sumOfSquaredCellValue.toDouble / numCells.toDouble) - (mean.toDouble * mean.toDouble)).toDouble

 spark.udf.register("NeighboursCount", (maxX: Int, maxY: Int, maxZ: Int, minX: Int, minY: Int, minZ: Int, inputX: Int, inputY: Int, inputZ: Int)
 => HotcellUtils.CountNeighbours(maxX, maxY, maxZ, minX, minY, minZ, inputX, inputY, inputZ))

 val neighbours = spark.sql("select NeighboursCount(" + minX + ", " + minY + ", " + minZ + ", " + maxX + ", " + maxY + "," + maxZ + "," + "cell1.x, cell1.y, cell1.z) as CountOfNeighbour, " +
 "cell1.x as x, cell1.y as y, cell1.z as z, sum(cell2.countHotCells) as SumHotCells " +
 "from cellHotnessInfo as cell1, cellHotnessInfo as cell2 " +
 "where (cell2.x = cell1.x+1 or cell2.x = cell1.x or cell2.x = cell1.x-1) " +
 "and (cell2.y = cell1.y+1 or cell2.y = cell1.y or cell2.y = cell1.y-1) " +
 "and (cell2.z = cell1.z+1 or cell2.z = cell1.z or cell2.z = cell1.z-1) " +
 "group by cell1.z, cell1.y, cell1.x order by cell1.z, cell1.y, cell1.x desc")
 neighbours.createOrReplaceTempView("neighbours");

 spark.udf.register("GScore", (x: Int, y: Int, z: Int, mean: Double, standardDeviation: Double, CountOfNeighbour: Int, SumHotCells: Int, numCells: Int)
 => HotcellUtils.getStatistic(x, y, z, mean, standardDeviation, CountOfNeighbour, SumHotCells, numCells))

 val NeighboursInfo = spark.sql("select GScore(x, y, z," + mean + "," + standardDeviation + ",CountOfNeighbour, SumHotCells," + numCells + ") as statistics, x, y, z from neighbours order by statistics desc");
 NeighboursInfo.createOrReplaceTempView("GScore")

 val finalResult = spark.sql("select x, y, z from GScore")
 finalResult.createOrReplaceTempView("result")
 return finalResult
 }
}
