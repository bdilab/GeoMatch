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

object MatchGeometryRange extends MatchGeometry {

    override def getMatchesContainer() =
        SortedSet[(Double, GMGeomBase)]()

    override def processMatches(jtsGeom: Geometry, lstTreeMatches: List[_], setMatches: Set[(Double, GMGeomBase)]) {

        val iter = lstTreeMatches.iterator()

        while (iter.hasNext()) {

            val geomMatch = iter.next() match {
                case geom: Geometry => geom
            }

            if (jtsGeom.intersects(geomMatch))
                setMatches.add((0, geomMatch.getUserData match { case x: GMGeomBase => x }))
        }
    }

    def apply(partItr: Iterator[(GeomKey, GMGeomBase)], errorRangeBy: Double) =
        super.runMatch(partItr, errorRangeBy, 0)

}