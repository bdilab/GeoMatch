/**
  * Copyright (c) 2019, The City University of New York and the University of Helsinki
  * All rights reserved.
  */

package org.cusp.bdi.gm.ops

import java.util.List

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Set

import org.cusp.bdi.gm.geom.GMGeomBase
import org.cusp.bdi.gm.keyPart.GeomKey
import org.cusp.bdi.gm.keyPart.GeomKeySetOne
import org.cusp.bdi.gm.keyPart.GeomKeySetTwoSkip
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.index.strtree.STRtree

trait MatchGeometry {

    def processMatches(jtsGeom: Geometry, lstTreeMatches: List[_], setMatches: Set[(Double, GMGeomBase)])

    def getMatchesContainer(): Set[(Double, GMGeomBase)]

    /**
      * Match each geometry to the nearest X geometries
      */
    def runMatch(partItr: Iterator[(GeomKey, GMGeomBase)], errorRangeBy: Double, matchGeometryCount: Int) = {

        val jtsGeomFact = new GeometryFactory()

        //        var rtreeGMGeomBase = RTree[Geometry]()
        val rtreeGMGeomBase = new STRtree()
        var ret: (GMGeomBase, ListBuffer[GMGeomBase]) = null

        partItr.map(tKeyGeom => {

            ret = null

            tKeyGeom._1 match {

                case _: GeomKeySetOne =>
                    tKeyGeom._2
                        .toJTS(jtsGeomFact)
                        .foreach(jtsGeom => {

                            val env = jtsGeom.getEnvelopeInternal
                            jtsGeom.setUserData(tKeyGeom._2)
                            env.expandBy(errorRangeBy)

                            rtreeGMGeomBase.insert(env, jtsGeom)
                        })
                case _: GeomKeySetTwoSkip => ret = (tKeyGeom._2, ListBuffer[GMGeomBase]())
                case _ => {

                    var listBestMatches = ListBuffer[GMGeomBase]()

                    if (!rtreeGMGeomBase.isEmpty()) {

                        val setMatches = getMatchesContainer()

                        tKeyGeom._2.toJTS(jtsGeomFact)
                            .foreach(jtsGeom => {

                                val env = jtsGeom.getEnvelopeInternal

                                val seqTreeMatches = rtreeGMGeomBase.query(env)

                                processMatches(jtsGeom, seqTreeMatches, setMatches)
                            })

                        var iter = setMatches.iterator

                        while (iter.hasNext && listBestMatches.size < matchGeometryCount) {

                            val geom = iter.next()._2

                            if (!listBestMatches.contains(geom))
                                listBestMatches.append(geom)
                        }
                    }
                    ret = (tKeyGeom._2, listBestMatches)
                }
            }

            ret
        })
            .filter(_ != null)
    }
}