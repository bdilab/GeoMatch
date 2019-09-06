/**
  * Copyright (c) 2019, The City University of New York and the University of Helsinki
  * All rights reserved.
  */

package org.cusp.bdi.gm.ops

import java.util.List

import scala.collection.mutable.Set
import scala.collection.mutable.SortedSet

import org.cusp.bdi.gm.geom.GMGeomBase
import org.cusp.bdi.gm.keyPart.GeomKey
import org.locationtech.jts.geom.Geometry

object MatchGeometryKNNDistance extends MatchGeometry {

    private var maxDistance = Double.MaxValue

    override def getMatchesContainer() =
        SortedSet[(Double, GMGeomBase)]()

    override def processMatches(jtsGeom: Geometry, lstTreeMatches: List[_], setMatches: Set[(Double, GMGeomBase)]) {

        val iter = lstTreeMatches.iterator()

        while (iter.hasNext()) {

            val geomMatch = iter.next() match {
                case geom: Geometry => geom
            }

            val dist = geomMatch.distance(jtsGeom)

            if (dist <= maxDistance)
                setMatches.add((dist, geomMatch.getUserData match { case x: GMGeomBase => x }))
        }
    }

    def apply(partItr: Iterator[(GeomKey, GMGeomBase)], errorRangeBy: Double, matchGeometryCount: Int, maxDistance: Double) = {

        this.maxDistance = maxDistance

        super.runMatch(partItr, errorRangeBy, matchGeometryCount)
    }

    def apply(partItr: Iterator[(GeomKey, GMGeomBase)], errorRangeBy: Double, matchGeometryCount: Int) =
        super.runMatch(partItr, errorRangeBy, matchGeometryCount)
}