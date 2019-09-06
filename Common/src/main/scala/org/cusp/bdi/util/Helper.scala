/**
  * Copyright (c) 2019, The City University of New York and the University of Helsinki
  * All rights reserved.
  */

package org.cusp.bdi.util

import java.io.File

import scala.collection.mutable.ListBuffer
import scala.util.Random

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object Helper {

    def isNullOrEmpty(arr: Array[_]) =
        arr == null || arr.isEmpty

    def isNullOrEmpty(list: ListBuffer[_]) =
        list == null || list.isEmpty

    def isNullOrEmpty(str: String) =
        str == null || str.isEmpty

    def isNullOrEmpty(str: Object*) = {

        var res = false

        if (str == null)
            true
        else
            str.filter(x => x == null || x.toString.length == 0).length == str.length
    }

    /**
      * Returns true is the parameter represents a "True" value as defined in LST_BOOL_VALS, false otherwise
      */
    def isBooleanStr(objBool: Object) = {

        if (isNullOrEmpty(objBool))
            false
        else {

            val ch = objBool.toString().charAt(0).toUpper

            ch == 'T' || ch == 'Y'
        }
    }

    def indexOf(str: String, searchStr: String): Int =
        indexOf(str, searchStr, 1, 0)

    def indexOf(str: String, searchStr: String, nth: Int): Int =
        indexOfCommon(new StringLikeObj(str, null), searchStr, nth, 0)

    def indexOf(str: String, searchStr: String, n: Int, startIdx: Int) =
        indexOfCommon(new StringLikeObj(str, null), searchStr, n, startIdx)

    def indexOf(strBuild: StringBuilder, searchStr: String, nth: Int): Int =
        indexOfCommon(new StringLikeObj(null, strBuild), searchStr, nth, 0)

    def indexOf(strBuild: StringBuilder, searchStr: String, n: Int, startIdx: Int) =
        indexOfCommon(new StringLikeObj(null, strBuild), searchStr, n, startIdx)

    case class StringLikeObj(stringObj: String, strBuildObj: StringBuilder) {
        def indexOf(str: String, fromIndex: Int) = {
            if (strBuildObj != null)
                strBuildObj.indexOf(str, fromIndex)
            else
                stringObj.indexOf(str, fromIndex)
        }
    }

    /**
      * Returns the nth index of, or -1 if out of range
      */
    private def indexOfCommon(strOrSBuild: StringLikeObj, searchStr: String, n: Int, startIdx: Int) = {

        var idx = startIdx - 1
        var break = false
        var i = 0

        while (i < n && !break) {

            idx = strOrSBuild.indexOf(searchStr, idx + 1)

            if (idx == -1)
                break = true
            i += 1
        }

        idx
    }

    def getMBREnds(arrCoords: Array[(Int, Int)], expandBy: Int) = {

        val xCoords = arrCoords.map(_._1)
        val yCoords = arrCoords.map(_._2)

        var minX = xCoords.min - expandBy
        var minY = yCoords.min - expandBy
        var maxX = xCoords.max + expandBy
        var maxY = yCoords.max + expandBy

        // Closed ring MBR (1st and last points repeated)
        Array((minX, minY), (maxX, maxY))
    }

    def getMBREnds(arrCoords: Array[(Float, Float)], expandBy: Float) = {

        val xCoords = arrCoords.map(_._1)
        val yCoords = arrCoords.map(_._2)

        var minX = xCoords.min - expandBy
        var minY = yCoords.min - expandBy
        var maxX = xCoords.max + expandBy
        var maxY = yCoords.max + expandBy

        // Closed ring MBR (1st and last points repeated)
        Array((minX, minY), (maxX, maxY))
    }

    def getMBR_ClosedRing(arrCoords: Array[(Double, Double)], expandBy: Double) = {

        val xCoords = arrCoords.map(_._1)
        val yCoords = arrCoords.map(_._2)

        var minX = xCoords.min - expandBy
        var maxX = xCoords.max + expandBy
        var minY = yCoords.min - expandBy
        var maxY = yCoords.max + expandBy

        // Closed ring MBR (1st and last points repeated)
        Array((minX, minY), (maxX, minY), (maxX, maxY), (minX, maxY), (minX, minY))
    }

    //    def getMBR(arrCoords: Array[(Int, Int)], expandBy: Int) = {
    //
    //        val xCoords = arrCoords.map(_._1)
    //        val yCoords = arrCoords.map(_._2)
    //
    //        var minX = xCoords.min - expandBy
    //        var maxX = xCoords.max + expandBy
    //        var minY = yCoords.min - expandBy
    //        var maxY = yCoords.max + expandBy
    //
    //        // Closed ring MBR (1st and last points repeated)
    //        Array((minX, minY), (maxX, minY), (maxX, maxY), (minX, maxY))
    //    }
    //
    //
    //    def getMBR(arrCoords: Array[(Int, Int)], expandBy: Float, closedRing: Boolean) = {
    //
    //        val xCoords = arrCoords.map(_._1)
    //        val yCoords = arrCoords.map(_._2)
    //
    //        var minX = xCoords.min - expandBy
    //        var maxX = xCoords.max + expandBy
    //        var minY = yCoords.min - expandBy
    //        var maxY = yCoords.max + expandBy
    //
    //        if (closedRing) // Closed ring MBR (1st and last points repeated)
    //            Array((minX, minY), (maxX, minY), (maxX, maxY), (minX, maxY), (minX, minY))
    //        else
    //            Array((minX, minY), (maxX, minY), (maxX, maxY), (minX, maxY))
    //    }

    /**
      * Sends message(s) to the log belonging to the class when debug is turned on
      */
    def logMessage(debugOn: Boolean, clazz: => Any, message: => Object) {
        if (debugOn)
            Logger.getLogger(clazz.getClass().getName).info("# " + message)
    }

    /**
      * Randomizes the output directory. This is used when running Spark in local mode for testing
      */
    def randOutputDir(outputDir: String) = {

        var randOutDir = StringBuilder.newBuilder
            .append(outputDir)

        if (!outputDir.endsWith(File.separator))
            randOutDir.append(File.separator)

        randOutDir.append(Random.nextInt(999))

        randOutDir.toString()
    }

    def delDirHDFS(sparkContext: SparkContext, dirName: String) {

        try {
            val hdfs = FileSystem.get(sparkContext.hadoopConfiguration)
            val path = new Path(dirName)
            if (hdfs.exists(path))
                hdfs.delete(path, true)
        }
        catch {
            case ex: Exception => ex.printStackTrace()
        }
    }

    def ensureClosedRingCoordinates[T <: AnyVal](arrCoord: Array[(T, T)]) = {

        var retArr = arrCoord

        while (retArr.length < 4 || retArr.head._1 != retArr.last._1 || retArr.head._2 != retArr.last._2)
            retArr = retArr :+ arrCoord.head

        retArr
    }
}
