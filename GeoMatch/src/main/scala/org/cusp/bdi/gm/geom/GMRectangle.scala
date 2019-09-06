/**
  * Copyright (c) 2019, The City University of New York and the University of Helsinki
  * All rights reserved.
  */

package org.cusp.bdi.gm.geom

import scala.collection.mutable.HashSet

import org.cusp.bdi.gm.geom.util.LineRasterization
import org.cusp.bdi.gm.geom.util.PolygonRasterization
import org.cusp.bdi.util.HilbertIndex
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.GeometryFactory

/**
  * A class representing a LineString/MultiLineString objects
  */
case class GMRectangle(_payload: String, lowEnd: (Int, Int), highEnd: (Int, Int)) extends GMPolygonBase(_payload, Array(lowEnd, highEnd)) {

    // A 5-point coordinate array representing the rectangle. The last and first coordinates are repeated (for JTS)
    private val closedRingCoord = Array(coordArr(0), (coordArr(1)._1, coordArr(0)._2), coordArr(1), (coordArr(0)._1, coordArr(1)._2), coordArr(0))

    override def toJTS(jtsGeomFact: GeometryFactory) =
        Seq(jtsGeomFact.createPolygon(closedRingCoord.map(coord => new Coordinate(coord._1, coord._2))))

    override def getHilbertIndexList(hilbertBoxWidth: Int, hilbertBoxHeight: Int, searchGridMinX: Int, searchGridMinY: Int, hilbertN: Int) = {

        val coordArr = closedRingCoord

        var hCoordStart = computeHilberXY(hilbertBoxWidth, hilbertBoxHeight, searchGridMinX, searchGridMinY, coordArr(0))

        val coordSet = HashSet[(Int, Int)](hCoordStart)

        (1 until coordArr.length - 1)
            .foreach(i => {

                var hCoordEnd = computeHilberXY(hilbertBoxWidth, hilbertBoxHeight, searchGridMinX, searchGridMinY, coordArr(i))

                coordSet += hCoordEnd

                hCoordStart = hCoordEnd
            })

        coordSet.size match {
            case 1 => HashSet(HilbertIndex.computeIndex(hilbertN, computeHilberXY(hilbertBoxWidth, hilbertBoxHeight, searchGridMinX, searchGridMinY, coordArr(0))))
            case 2 => LineRasterization(coordSet.head, coordSet.last)
                .map(t => HilbertIndex.computeIndex(hilbertN, t))
            case _ => {

                val coordList = coordSet.toList

                new PolygonRasterization(coordList.map(_._1), coordList.map(_._2)).getCoordSet()
                    .map(t => HilbertIndex.computeIndex(hilbertN, t))
            }
        }
    }
}