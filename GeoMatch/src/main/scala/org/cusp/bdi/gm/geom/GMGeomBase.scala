/**
  * Copyright (c) 2019, The City University of New York and the University of Helsinki
  * All rights reserved.
  */

package org.cusp.bdi.gm.geom

import scala.collection.mutable.Set

import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory

/**
  * A base class for all geometry classes
  */
abstract class GMGeomBase(_payload: String, _coordArr: Array[(Int, Int)]) extends Serializable with Ordered[GMGeomBase] {

    def payload = _payload
    def coordArr = _coordArr

    override def compare(other: GMGeomBase) =
        payload.compareTo(other.payload)

    override def equals(other: Any) = other match {
        case x: GMGeomBase => payload.equals(x.payload)
        case _ => false
    }

    /**
      * Returns a JTS representation of the object using the GeometryFactory
      */
    def toJTS(jtsGeomFact: GeometryFactory): Seq[Geometry]

    def getHilbertIndexList(hilbertBoxWidth: Int, hilbertBoxHeight: Int, searchGridMinX: Int, searchGridMinY: Int, hilbertN: Int): Set[Int]

    protected def computeHilberXY(hilbertBoxWidth: Int, hilbertBoxHeight: Int, searchGridMinX: Int, searchGridMinY: Int, pointCoord: (Int, Int)) =
        (math.floor((pointCoord._1 - searchGridMinX) / hilbertBoxWidth).toInt, math.floor((pointCoord._2 - searchGridMinY) / hilbertBoxHeight).toInt)
} 