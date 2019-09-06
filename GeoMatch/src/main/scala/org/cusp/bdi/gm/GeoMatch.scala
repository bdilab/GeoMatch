/**
  * Copyright (c) 2019, The City University of New York and the University of Helsinki
  * All rights reserved.
  */

package org.cusp.bdi.gm

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Set

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.cusp.bdi.util.HilbertIndex
import org.cusp.bdi.gm.geom.util.PolygonRasterization
import org.locationtech.jts.geom.LineString
import org.locationtech.jts.geom.Geometry
import org.cusp.bdi.gm.geom.GMPoint
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.Coordinate
import org.cusp.bdi.gm.geom.GMLineString
import org.cusp.bdi.gm.geom.GMPolygon
import org.cusp.bdi.gm.keyPart.GMPartitioner
import org.cusp.bdi.gm.geom.GMGeomBase
import org.cusp.bdi.util.CLArgsParser
import org.cusp.bdi.gm.keyPart.PartitionMapper
import org.cusp.bdi.gm.keyPart.GeomKeySetTwoSkip
import org.cusp.bdi.gm.ops.MatchGeometryKNNDistance
import org.cusp.bdi.gm.keyPart.GeomKeySetOne
import org.cusp.bdi.gm.ops.MatchGeometryRange
import org.cusp.bdi.gm.geom.util.LineRasterization
import org.cusp.bdi.gm.keyPart.GeomKey
import org.cusp.bdi.gm.keyPart.GeomKeySetTwo
import org.cusp.bdi.gm.ops.MatchGeometry
import org.locationtech.jts.geom.Point
import org.locationtech.jts.geom.Polygon
import org.cusp.bdi.util.Helper

object GeoMatch {

    def getGeoMatchClasses(): Array[Class[_]] =
        Array(classOf[Coordinate],
              classOf[GeometryFactory],
              classOf[GeomKey],
              classOf[GeomKeySetOne],
              classOf[GeomKeySetTwo],
              classOf[GeomKeySetTwoSkip],
              classOf[GMGeomBase],
              classOf[GMLineString],
              classOf[GMPartitioner],
              classOf[GMPoint],
              classOf[GMPolygon],
              classOf[LineString],
              classOf[GeometryFactory],
              classOf[Geometry],
              classOf[MatchGeometry],
              classOf[Point],
              classOf[Polygon],
              classOf[String],
              CLArgsParser.getClass,
              GeoMatch.getClass,
              HilbertIndex.getClass,
              LineRasterization.getClass,
              MatchGeometryKNNDistance.getClass,
              MatchGeometryRange.getClass,
              PartitionMapper.getClass,
              PolygonRasterization.getClass,
              Helper.getClass)

}

case class GeoMatch(outputDebugMessages: Boolean = false, hilbertN: Int = 256, errorRangeBy: Double = 150, searchGridMBR: (Int, Int, Int, Int) = (-1, -1, -1, -1)) extends Serializable {

    /**
      * Returns an RDD of geometries such that each geometry object from the second set is accompanied by a set of geometries
      * within its range from the first set
      */
    def spatialJoinRange[T <: GMGeomBase, U <: GMGeomBase](rddFirstSet: RDD[T], rddSecondSet: RDD[U], adjustForBoundryCrossing: Boolean = false): RDD[(U, ListBuffer[T])] = {

        Helper.logMessage(outputDebugMessages, GeoMatch, "Running spatialJoinRange")

        cluster(rddFirstSet, rddSecondSet, 0, adjustForBoundryCrossing)
            .mapPartitions(partItr => MatchGeometryRange(partItr, errorRangeBy)
                .map(row => (row._1.asInstanceOf[U], row._2.asInstanceOf[ListBuffer[T]])))
    }

    /**
      * Returns an RDD of geometries such that each geometry object from the second set is accompanied by its nearest k geometries
      */
    def spatialJoinKNN[T <: GMGeomBase, U <: GMGeomBase](rddFirstSet: RDD[T], rddSecondSet: RDD[U], k: Int, adjustForBoundryCrossing: Boolean = false): RDD[(U, ListBuffer[T])] = {

        Helper.logMessage(outputDebugMessages, GeoMatch, "Running spatialJoinKNN")

        cluster(rddFirstSet, rddSecondSet, k, adjustForBoundryCrossing)
            .mapPartitions(partItr => MatchGeometryKNNDistance(partItr, errorRangeBy, k)
                .map(row => (row._1.asInstanceOf[U], row._2.asInstanceOf[ListBuffer[T]])))
    }

    /**
      * Returns an RDD of geometries such that each geometry object from the second set is accompanied by a set of geometries (size k)
      * within the specified distance from the first set
      */
    def spatialJoinDistance[T <: GMGeomBase, U <: GMGeomBase](rddFirstSet: RDD[T], rddSecondSet: RDD[U], k: Int = 3, matchGeometryMaxDist: Double = 150, adjustForBoundryCrossing: Boolean = false): RDD[(U, ListBuffer[T])] = {

        Helper.logMessage(outputDebugMessages, GeoMatch, "Running spatialJoinDistance")

        cluster(rddFirstSet, rddSecondSet, k, adjustForBoundryCrossing)
            .mapPartitions(partItr => MatchGeometryKNNDistance(partItr, errorRangeBy, k, matchGeometryMaxDist)
                .map(row => (row._1.asInstanceOf[U], row._2.asInstanceOf[ListBuffer[T]])))
    }

    private def cluster[T <: GMGeomBase, U <: GMGeomBase](rddFirstSet: RDD[T], rddSecondSet: RDD[U], matchGeometryCount: Int = 3, adjustForBoundryCrossing: Boolean = false): RDD[(GeomKey, GMGeomBase)] = {

        var (searchGridMinX, searchGridMinY, searchGridMaxX, searchGridMaxY) = searchGridMBR

        // compute search grid region if not provided
        if (searchGridMinX == -1 || searchGridMinY == -1 || searchGridMaxX == -1 || searchGridMaxY == -1) {

            val mbr = calculateSearchGridMBR(rddFirstSet)

            searchGridMinX = mbr._1
            searchGridMinY = mbr._2
            searchGridMaxX = mbr._3
            searchGridMaxY = mbr._4
        }

        if (searchGridMinX > searchGridMaxX || searchGridMinY > searchGridMaxY)
            throw new IllegalArgumentException("Invalid MBR Min/Max values")

        Helper.logMessage(outputDebugMessages, GeoMatch, "Search Grid MBR: (%d, %d, %d, %d)".format(searchGridMinX, searchGridMinY, searchGridMaxX, searchGridMaxY))

        val hilbertBoxWidth = (searchGridMaxX - searchGridMinX) / hilbertN
        val hilbertBoxHeight = (searchGridMaxY - searchGridMinY) / hilbertN

        var arrPartMapping = PartitionMapper.getArrIndexPartMapping(hilbertN, hilbertBoxWidth, hilbertBoxHeight, searchGridMinX, searchGridMinY, rddSecondSet)

        if (adjustForBoundryCrossing) {

            Helper.logMessage(outputDebugMessages, GeoMatch, "Adjusting partiion mapping for boundery crossing objects")

            PartitionMapper.adjustForBoundryCrossing(rddFirstSet, arrPartMapping, matchGeometryCount, hilbertBoxWidth, hilbertBoxHeight, searchGridMinX, searchGridMinY, hilbertN)
        }

        val keyPart = new GMPartitioner(rddSecondSet.getNumPartitions)

        val rddFirstSetKeyGeom = rddFirstSet
            .mapPartitions(_.map(geom => {

                geom.getHilbertIndexList(hilbertBoxWidth, hilbertBoxHeight, searchGridMinX, searchGridMinY, hilbertN)
                    .map(hIdx => {

                        val partInfo = PartitionMapper.partitionLookup(-1, hIdx, arrPartMapping)

                        partInfo._2.add(partInfo._1)

                        partInfo._2.filter(_ != -1)
                    })
                    .flatMap(_.seq)
                    .map(partNum => {

                        val gmGeomKey: GeomKey = new GeomKeySetOne(partNum)
                        val gmGeom: GMGeomBase = geom

                        (gmGeomKey, gmGeom)
                    })
            }))
            .flatMap(_.seq)
            .partitionBy(keyPart)

        val rddSecondSetKeyGeom = rddSecondSet.mapPartitionsWithIndex((partNum, iter) => iter.map(geom => {

            val setIdx = geom
                .getHilbertIndexList(hilbertBoxWidth, hilbertBoxHeight, searchGridMinX, searchGridMinY, hilbertN)
                .filter(_ != -1)

            var gmGeomKey: GeomKey = null
            val gmGeom: GMGeomBase = geom

            if (setIdx.isEmpty)
                gmGeomKey = new GeomKeySetTwoSkip(partNum)
            else {

                val partInfo = PartitionMapper.partitionLookup(partNum, setIdx.head, arrPartMapping)

                gmGeomKey = new GeomKeySetTwo(partInfo._1)
            }

            (gmGeomKey, gmGeom)
        }), true)
            .partitionBy(keyPart)

        rddFirstSetKeyGeom.union(rddSecondSetKeyGeom)
    }

    private def calculateSearchGridMBR[T <: GMGeomBase](rddFirstSet: RDD[T]) = {

        rddFirstSet
            .mapPartitions(_.map(_.coordArr))
            .flatMap(_.seq)
            .mapPartitions(iter => {

                var (minX: Int, minY: Int, maxX: Int, maxY: Int) =
                    iter.fold((Int.MaxValue, Int.MaxValue, Int.MinValue, Int.MinValue))((x, y) => {

                        val (a, b, c, d) = x.asInstanceOf[(Int, Int, Int, Int)]
                        val t = y.asInstanceOf[(Int, Int)]

                        (math.min(a, t._1), math.min(b, t._2), math.max(c, t._1), math.max(d, t._2))
                    })

                Iterator((minX, minY, maxX, maxY))
            })
            .fold((Int.MaxValue, Int.MaxValue, Int.MinValue, Int.MinValue))((x, y) => {

                val (a, b, c, d) = x.asInstanceOf[(Int, Int, Int, Int)]
                val t = y.asInstanceOf[(Int, Int, Int, Int)]

                (math.min(a, t._1), math.min(b, t._2), math.max(c, t._3), math.max(d, t._4))
            })
    }
}