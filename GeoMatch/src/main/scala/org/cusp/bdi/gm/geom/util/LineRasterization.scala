/**
  * Copyright (c) 2019, The City University of New York and the University of Helsinki
  * All rights reserved.
  */

package org.cusp.bdi.gm.geom.util

import scala.collection.mutable.HashSet
import scala.collection.mutable.ListBuffer

object LineRasterization {

    def apply(coordStart: (Int, Int), coordEnd: (Int, Int)) = {

        val setCoordsLine = HashSet[(Int, Int)]()

        var x0 = coordStart._1 << 1 // * 2
        var y0 = coordStart._2 << 1 // * 2

        val xEnd = coordEnd._1 << 1 // * 2
        val yEnd = coordEnd._2 << 1 // * 2

        val dx = Math.abs(xEnd - x0)
        val dy = Math.abs(yEnd - y0)

        val sx = if (x0 < xEnd) 1 else -1
        val sy = if (y0 < yEnd) 1 else -1

        var err = dx - dy
        var e2 = 0

        appendToSet(x0, y0, setCoordsLine)

        while (x0 != xEnd || y0 != yEnd) {

            e2 = err << 1 // * 2

            if (e2 > -dy) {
                err = err - dy
                x0 += sx
            }

            if (e2 < dx) {
                err = err + dx
                y0 += sy
            }

            appendToSet(x0, y0, setCoordsLine)
        }

        setCoordsLine
    }

    private def appendToSet(x0: Int, y0: Int, setCoordsLine: HashSet[(Int, Int)]) = {

        val arrX = ListBuffer[Int]()
        val arrY = ListBuffer[Int]()

        arrX.append(x0 >> 1) // divide 2

        if ((x0 & 1) == 1)
            arrX.append(arrX.head + 1)

        arrY.append(y0 >> 1) // divide 2

        if ((y0 & 1) == 1)
            arrY.append(arrY.head + 1)

        arrX.foreach(x => arrY.foreach(y => setCoordsLine.add(x, y)))
    }
}