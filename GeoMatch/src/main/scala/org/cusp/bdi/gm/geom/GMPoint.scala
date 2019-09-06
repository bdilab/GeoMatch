/**
  * Copyright (c) 2019, The City University of New York and the University of Helsinki
  * All rights reserved.
  */

package org.cusp.bdi.gm.geom

import scala.collection.mutable.HashSet

import org.cusp.bdi.util.HilbertIndex
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory

/**
  * A class representing a LineString/MultiLineString objects
  */
case class GMPoint(_payload: String, _pointCoord: (Int, Int)) extends GMGeomBase(_payload, Array(_pointCoord)) {

    override def toJTS(jtsGeomFact: GeometryFactory): Seq[Geometry] =
        Array(jtsGeomFact.createPoint(new Coordinate(coordArr(0)._1, coordArr(0)._2)))

    override def getHilbertIndexList(hilbertBoxWidth: Int, hilbertBoxHeight: Int, searchGridMinX: Int, searchGridMinY: Int, hilbertN: Int) =
        HashSet(HilbertIndex.computeIndex(hilbertN, computeHilberXY(hilbertBoxWidth, hilbertBoxHeight, searchGridMinX, searchGridMinY, coordArr(0))))
}