/**
  * Copyright (c) 2019, The City University of New York and the University of Helsinki
  * All rights reserved.
  */

package org.cusp.bdi.gm.geom

import scala.collection.mutable.HashSet

import org.cusp.bdi.gm.geom.util.LineRasterization
import org.cusp.bdi.util.HilbertIndex
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.GeometryFactory

/**
  * A class representing a LineString objects
  */
case class GMLineString(_payload: String, _coordArr: Array[(Int, Int)]) extends GMGeomBase(_payload, _coordArr) {

    override def toJTS(jtsGeomFact: GeometryFactory) = {

        var xyStart: (Int, Int) = coordArr(0)

        (1 until coordArr.length).map(i => {

            var xyEnd = coordArr(i)

            var lineStr = jtsGeomFact.createLineString(Array(new Coordinate(xyStart._1, xyStart._2), new Coordinate(xyEnd._1, xyEnd._2)))

            xyStart = xyEnd

            lineStr
        })
    }

    override def getHilbertIndexList(hilbertBoxWidth: Int, hilbertBoxHeight: Int, searchGridMinX: Int, searchGridMinY: Int, hilbertN: Int) = {

        val setStreetHilbertIdx = HashSet[Int]()

        var xyStart = computeHilberXY(hilbertBoxWidth, hilbertBoxHeight, searchGridMinX, searchGridMinY, coordArr(0))

        (1 until coordArr.length)
            .foreach(i => {

                var xyEnd = computeHilberXY(hilbertBoxWidth, hilbertBoxHeight, searchGridMinX, searchGridMinY, coordArr(i))

                // run rasterization for each street segment (startXY, endXY)
                LineRasterization(xyStart, xyEnd)
                    .foreach(hilbertPoint => setStreetHilbertIdx += HilbertIndex.computeIndex(hilbertN, hilbertPoint))

                xyStart = xyEnd
            })

        setStreetHilbertIdx
    }
}