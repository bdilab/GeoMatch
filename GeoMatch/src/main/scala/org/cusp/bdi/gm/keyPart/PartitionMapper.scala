/**
  * Copyright (c) 2019, The City University of New York and the University of Helsinki
  * All rights reserved.
  */

package org.cusp.bdi.gm.keyPart

import scala.collection.mutable.HashSet
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Set

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.cusp.bdi.gm.geom.GMGeomBase
import org.cusp.bdi.util.HilbertIndex

/**
  * Builds a fair partition distribution scheme based on the Hilbert box, number of points in each index, and number of partitions
  */
object PartitionMapper {

    def getArrIndexPartMapping(hilbertN: Int, hilbertBoxWidth: Int, hilbertBoxHeight: Int, searchGridMinX: Int, searchGridMinY: Int, rddSecondSet: RDD[_ <: GMGeomBase]) = {

        val numPartitions = rddSecondSet.getNumPartitions

        val arrIdxSummary = rddSecondSet
            .mapPartitions(_.map(gmGeom => {

                val setHIdx = gmGeom.getHilbertIndexList(hilbertBoxWidth, hilbertBoxHeight, searchGridMinX, searchGridMinY, hilbertN)
                    .filter(_ != -1)

                setHIdx.map(hIdx => {

                    val setHIdxFinal = setHIdx
                    setHIdxFinal.remove(hIdx)

                    (hIdx, (1L, setHIdxFinal))
                })
            })
                .flatMap(_.seq))
            .reduceByKey((x, y) => (x._1 + y._1, y._2 ++ x._2))
            .sortByKey()
            .collect()

        val arrPartMapping = computeArrPartMapping(arrIdxSummary, numPartitions)

        // convert HashSet from Hilbert indexes to partition numbers
        arrPartMapping.map(row => {

            val setPartition = row._4.map(hIdx => partitionLookup(-999, hIdx, arrPartMapping)._1)

            setPartition.remove(row._3)

            (row._1, row._2, row._3, setPartition)
        })
    }

    def partitionLookup(currPartNum: Int, hIdx: Int, arrPartMapping: Array[(Int, Int, Int, Set[Int])]) = {

        var topIdx = 0
        var botIdx = arrPartMapping.size - 1
        var retPart = (-1, Set[Int]())

        while (retPart._1 == -1 && botIdx >= topIdx) {

            val midIdx = (topIdx + botIdx) / 2
            val (hIdxFrom, hIdxTo, partNum, setIdx) = arrPartMapping(midIdx)

            if (hIdxFrom <= hIdx && hIdx <= hIdxTo)
                retPart = (partNum, setIdx)
            else if (hIdx < hIdxFrom)
                botIdx = midIdx - 1
            else
                topIdx = midIdx + 1
        }

        retPart._1 match {
            case -1 => (currPartNum, Set[Int]())
            case _ => retPart
        }
    }

    private def computeArrPartMapping(arrIdxSummary: Array[(Int, (Long, Set[Int]))], numPartitions: Int) = {

        var partitionCounter = 0
        var count = 0L
        var setIdx = Set[Int]()
        var start = -1
        var tuple: (Int, Long, Set[Int]) = null

        var pointCount = arrIdxSummary.map(_._2._1).sum.toDouble
        val partitionLoad = math.ceil(pointCount / numPartitions).toLong
        var arrPartMapping = arrIdxSummary.map(row => {

            var ret: (Int, Int, Int, Set[Int]) = null

            tuple = (row._1, row._2._1, row._2._2)

            if (start == -1)
                start = tuple._1

            count += tuple._2
            tuple._3.foreach(setIdx.add)

            if (count >= partitionLoad) {

                ret = (start, tuple._1, partitionCounter, setIdx.filter(hIdx => (hIdx < start || hIdx > tuple._1)))

                count = 0
                setIdx = Set[Int]()
                start = -1
                partitionCounter += 1
            }

            ret
        })
            .filter(_ != null)

        // special case for last entries in the seq. If these last entries add up to < partitionLoad
        //    they will not be added in the above fore
        if (count > 0) {

            tuple._3.foreach(setIdx.add)
            arrPartMapping = arrPartMapping :+ (start, tuple._1, partitionCounter, setIdx)
        }

        arrPartMapping
    }

    /**
      * Expands the additional partitions sets to ensure that each partition group has at least 'matchGeometryCount' objects from the first set
      */
    def adjustForBoundryCrossing(rddFirstSet:      RDD[_ <: GMGeomBase],
                                 arrPartMapping:   Array[(Int, Int, Int, Set[Int])],
                                 minGeomCount:     Int,
                                 hilbertBoxWidth:  Int,
                                 hilbertBoxHeight: Int,
                                 searchGridMinX:   Int,
                                 searchGridMinY:   Int,
                                 hilbertN:         Int) {

        val mapSecondSetPartCounts = rddFirstSet
            .mapPartitions(_.map(gmGeom => gmGeom.getHilbertIndexList(hilbertBoxWidth, hilbertBoxHeight, searchGridMinX, searchGridMinY, hilbertN))
                .map(_.filter(_ != -1))
                .map(setHIdx => setHIdx.map(hIdx => (hIdx, 1L)))
                .flatMap(_.seq))
            .reduceByKey(_ + _)
            .sortByKey()
            .collect()
            .map(row => ((partitionLookup(-999, row._1, arrPartMapping)._1), row._2))
            .groupBy(_._1)
            .map(row => (row._1, row._2.map(_._2).sum))
        //        mapSecondSetPartCounts.foreach(x => Helper.logMessage(true, PartitionMapper, "<<mapSecondSetPartCounts<<%d,%d".format(x._1, x._2)))
        (0 until arrPartMapping.length).foreach(idx => {

            val row = arrPartMapping(idx)
            var firstSetCount = mapSecondSetPartCounts.getOrElse(row._3, 0L) +
                row._4.map(partNum => mapSecondSetPartCounts.getOrElse(partNum, 0L)).sum

            var lstHIndx = (row._1 to row._2).to[ListBuffer]
            var counter = 0

            // terminates when count is met or
            // when we run out of nearby indexes
            while (firstSetCount < minGeomCount && !lstHIndx.isEmpty && counter < lstHIndx.size) {

                // list of nearby indexes
                // ToDo: add cache
                val lstIndexNearby = HilbertIndex.getNearbyIndexes(hilbertN, lstHIndx(counter))
                    .filter(x => !lstHIndx.contains(x))

                // map to partitions and only keep the ones that are not already in the additional partitions list
                lstIndexNearby
                    .to[HashSet]
                    .map(x => {

                        val t = partitionLookup(-999, x, arrPartMapping)

                        (t._2 + t._1).filter(x => !(x == -999 || x == row._3 || row._4.contains(x)))
                    })
                    .flatMap(_.seq)
                    .foreach(partNum => {

                        row._4.add(partNum)

                        firstSetCount += mapSecondSetPartCounts.getOrElse(partNum, 0L)
                    })

                // next set of indexes to work with if needed
                lstIndexNearby.foreach(x => lstHIndx.append(x))
                counter += 1
            }
        })
    }
}