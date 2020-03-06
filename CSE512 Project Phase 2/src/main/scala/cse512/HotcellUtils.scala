package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
 val coordinateStep = 0.01

 def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int = {
 // Configuration variable:
 // Coordinate step is the size of each cell on x and y
 var result = 0
 coordinateOffset match {
 case 0 => result = Math.floor((inputString.split(",")(0).replace("(", "").toDouble / coordinateStep)).toInt
 case 1 => result = Math.floor(inputString.split(",")(1).replace(")", "").toDouble / coordinateStep).toInt
 // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
 case 2 => {
 val timestamp = HotcellUtils.timestampParser(inputString)
 result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
 }
 }
 return result
 }

 def timestampParser(timestampString: String): Timestamp = {
 val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
 val parsedDate = dateFormat.parse(timestampString)
 val timeStamp = new Timestamp(parsedDate.getTime)
 return timeStamp
 }

 def dayOfYear(timestamp: Timestamp): Int = {
 val calendar = Calendar.getInstance
 calendar.setTimeInMillis(timestamp.getTime)
 return calendar.get(Calendar.DAY_OF_YEAR)
 }

 def dayOfMonth(timestamp: Timestamp): Int = {
 val calendar = Calendar.getInstance
 calendar.setTimeInMillis(timestamp.getTime)
 return calendar.get(Calendar.DAY_OF_MONTH)
 }

 // YOU NEED TO CHANGE THIS PART

 def CountNeighbours(maximumX: Int, maximumY: Int, maximumZ: Int, minimumX: Int, minimumY: Int, minimumZ: Int, inputX: Int, inputY: Int, inputZ: Int): Int = {
 var count = 0;

 if (inputX == maximumX || inputX == minimumX)
 count += 1;

 if (inputY == maximumY || inputY == minimumY)
 count += 1;

 if (inputZ == maximumZ || inputZ == minimumZ)
 count += 1;

 if (count == 1) return 17
 else if (count == 2) return 11;
 else if (count == 3) return 7;
 else return 26;
 }

 def getStatistic(x: Int, y: Int, z: Int, mean: Double, standardDeviation: Double, CountOfNeighbour: Int, SumHotCells: Int, numcells: Int): Double = {
 val numerator = SumHotCells.toDouble - (mean * CountOfNeighbour.toDouble)
 val squareRoot = Math.sqrt((((numcells.toDouble * CountOfNeighbour.toDouble) - (CountOfNeighbour.toDouble * CountOfNeighbour.toDouble)) / (numcells.toDouble - 1.0).toDouble).toDouble).toDouble
 val denominator = standardDeviation * squareRoot
 return (numerator / denominator).toDouble
 }

 def square(a: Int): Double = {
 return (a * a).toDouble;
 }
}
