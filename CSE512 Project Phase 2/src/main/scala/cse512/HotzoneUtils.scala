package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
var rect = new Array[String](4)
    rect = queryRectangle.split(",")
    var rectangleX1 = rect(0).trim.toDouble
    var rectangleY1 = rect(1).trim.toDouble
    var rectangleX2 = rect(2).trim.toDouble
    var rectangleY2 = rect(3).trim.toDouble

    var point = new Array[String](2)
    point = pointString.split(",")
    var pointX = point(0).trim.toDouble
    var pointY = point(1).trim.toDouble

    var minimumX = 0.0
    var maximumX = 0.0

    if (rectangleX1 < rectangleX2) {
      minimumX = rectangleX1
      maximumX = rectangleX2
    }
    else {
      minimumX = rectangleX2
      maximumX = rectangleX1
    }

    var minimumY = math.min(rectangleY1, rectangleY2)
    var maximumY = math.max(rectangleY1, rectangleY2)

    if (pointY > maximumY || pointX < minimumX || pointX > maximumX || pointY < minimumY)
      return false

    return true
   // YOU NEED TO CHANGE THIS PART
  }

  // YOU NEED TO CHANGE THIS PART

}
