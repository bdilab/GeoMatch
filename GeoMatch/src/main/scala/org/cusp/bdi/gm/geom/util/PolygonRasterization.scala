/**
  * Copyright (c) 2019, The City University of New York and the University of Helsinki
  * All rights reserved.
  */

package org.cusp.bdi.gm.geom.util

import scala.collection.mutable.HashSet
import scala.collection.mutable.ListBuffer

/**
  * Scan line algorithm
  * The algorithm was translated and modified from GiHub (https://github.com/davidmigloz/photo-editor/blob/master/src/main/java/com/davidmiguel/photoeditor/drawing/ScanlineAlgorithm.java)
  */
case class PolygonRasterization(_coordArrX: List[Int], _coordArrY: List[Int]) {

    private var edges: Int = 0
    private var minYs: Array[Int] = null
    private var maxYs: Array[Int] = null
    private var xMinY: Array[Double] = null
    private var iSlopes: Array[Double] = null

    private var activeEdges: Int = 0
    private var goblalEdges: Array[Int] = null

    val coordArrX = _coordArrX.map(x => x << 1)
    val coordArrY = _coordArrY.map(y => y << 1)

    def getCoordSet() = {

        val n = coordArrX.length

        if (n > 1) {

            minYs = Array.fill(n) { 0 }
            maxYs = Array.fill(n) { 0 }
            xMinY = Array.fill(n) { 0.0 }
            iSlopes = Array.fill(n) { 0.0 }
            goblalEdges = Array.fill(n) { 0 }

            var i2 = 0
            var x1 = 0
            var x2 = 0
            var y1 = 0
            var y2 = 0

            edges = 0

            (0 until n).foreach(i1 => {

                i2 = if (i1 == n - 1) 0 else i1 + 1
                y1 = coordArrY(i1)
                y2 = coordArrY(i2)
                x1 = coordArrX(i1)
                x2 = coordArrX(i2)

                if (y1 != y2) {

                    if (y1 > y2) {

                        // Sort coords
                        var tmp = y1
                        y1 = y2
                        y2 = tmp
                        tmp = x1
                        x1 = x2
                        x2 = tmp
                    }

                    var iSlope = (x2 - x1) / (y2 - y1).toDouble
                    //                    if (minYs.length == edges || maxYs.length == edges)
                    //                        println()
                    minYs(edges) = y1
                    maxYs(edges) = y2
                    xMinY(edges) = if (y1 < y2) x1 else x2
                    iSlopes(edges) = iSlope
                    edges += 1
                }
            })

            activeEdges = 0

            getInnerCoordinates(coordArrY.max, coordArrY.min)
        }
        else
            HashSet((coordArrX.head, coordArrY.head))
    }

    private def activateEdges(y: Int) {

        (0 until edges).foreach(i => {

            val edge = i

            if (y == minYs(edge)) {

                var index = 0

                while (index < activeEdges && xMinY(edge) > xMinY(goblalEdges(index)))
                    index += 1

                for (j <- (activeEdges - 1) to index by -1)
                    goblalEdges(j + 1) = goblalEdges(j)

                goblalEdges(index) = edge
                activeEdges += 1
            }
        })
    }

    private def removeInactiveEdges(y: Int) {

        var i = 0
        while (i < activeEdges) {
            var index = goblalEdges(i)
            if (y < minYs(index) || y >= maxYs(index)) {

                (i until (activeEdges - 1))
                    .foreach(j => goblalEdges(j) = goblalEdges(j + 1))

                activeEdges -= 1
            }
            else
                i += 1
        }
    }

    private def updateXCoordinates() {

        var index = 0
        var x1 = -Double.MaxValue
        var x2 = 0.0
        var sorted = true
        (0 until activeEdges).foreach(i => {

            index = goblalEdges(i)
            x2 = xMinY(index) + iSlopes(index)
            xMinY(index) = x2
            if (x2 < x1)
                sorted = false

            x1 = x2
        })

        if (!sorted)
            sortActiveEdges()
    }

    private def sortActiveEdges() {

        var min = 0
        var tmp = 0

        (0 until activeEdges).foreach(i => {

            min = i

            (i until activeEdges).foreach(j => {
                if (xMinY(goblalEdges(j)) < xMinY(goblalEdges(min)))
                    min = j
            })

            tmp = goblalEdges(min)
            goblalEdges(min) = goblalEdges(i)
            goblalEdges(i) = tmp
        })
    }

    private def appendToSet(coord: (Int, Int), set: HashSet[(Int, Int)]) = {

        val arrX = ListBuffer[Int]()
        val arrY = ListBuffer[Int]()

        arrX.append(coord._1 >> 1) // divide 2

        if ((coord._1 & 1) == 1) // odd
            arrX.append(arrX.head + 1)

        arrY.append(coord._2 >> 1) // divide 2

        if ((coord._2 & 1) == 1)
            arrY.append(arrY.head + 1)

        arrX.foreach(x => arrY.foreach(y => set.add(x, y)))
    }

    private def getInnerCoordinates(maxY: Int, minY: Int) = {

        var setCoord = HashSet[(Int, Int)]()

        var x1 = 0
        var x2 = 0

        (minY to maxY).foreach(y => {

            removeInactiveEdges(y)
            activateEdges(y)

            for (i <- 0 until activeEdges by 2) {

                x1 = (xMinY(goblalEdges(i)) + 0.5).toInt
                x2 = (xMinY(goblalEdges(i + 1)) + 0.5).toInt

                (x1 until x2).foreach(x => appendToSet(((x, y)), setCoord))
            }

            updateXCoordinates()
        })

        (0 until coordArrX.length).foreach(i => appendToSet((coordArrX(i), coordArrY(i)), setCoord))

        setCoord
    }
}