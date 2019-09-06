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
abstract class GMPolygonBase(_payload: String, _coordArr: Array[(Int, Int)]) extends GMGeomBase(_payload, _coordArr) {

    override def toJTS(jtsGeomFact: GeometryFactory) = {

        val polygon = jtsGeomFact.createPolygon(coordArr.map(coord => new Coordinate(coord._1, coord._2)))

        Seq(polygon)
    }

    override def getHilbertIndexList(hilbertBoxWidth: Int, hilbertBoxHeight: Int, searchGridMinX: Int, searchGridMinY: Int, hilbertN: Int) = {

        var hCoordStart = computeHilberXY(hilbertBoxWidth, hilbertBoxHeight, searchGridMinX, searchGridMinY, coordArr(0))

        val coordSet = HashSet[(Int, Int)](hCoordStart)

        (1 until coordArr.length)
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