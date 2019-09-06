/**
  * Copyright (c) 2019, The City University of New York and the University of Helsinki
  * All rights reserved.
  */

package org.cusp.bdi.util

/**
  * Wrapper class for int pass-by-reference
  */
private case class RefInt(var i: Int) {}

/**
  * Computes the Hilbert Index for a given coordinates
  *
  * The class assumes that the square is divided into (n X n) cells. Where
  * n is a power of 2. The box's lower left coordinates are (0,0) and the upper right coordinates are (n − 1, n − 1).
  *
  * Code translated from https://en.wikipedia.org/wiki/Hilbert_curve
  */
object HilbertIndex {

    /**
      * Returns the index of the given Hilbert box coordinates
      *
      * If the indexes of the box are out of range, -1 is returned.
      * Else, a value between 0 and (n-1) will be returned
      *
      * @param n, Hilbert's n
      * @param xy, the X and Y coordinates of the Hilbert box
      */
    def computeIndex(n: Int, xy: (Int, Int)) = {

        // if xy are out of range, don't compute the index
        if (xy._1 < n && xy._2 < n)
            mapToIndex(n, new RefInt(xy._1), new RefInt(xy._2))
        else
            -1
    }

    def reverseIndex(n: Int, hIdx: Int) = {

        var rx = 0
        var ry = 0
        var t = hIdx

        val x = new RefInt(0)
        val y = new RefInt(0)

        var s = 1
        while (s < n) {

            rx = 1 & (t / 2)
            ry = 1 & (t ^ rx)
            rotate(s, x, y, rx, ry)
            x.i += s * rx
            y.i += s * ry
            t /= 4

            s *= 2
        }

        (x.i, y.i)
    }

    def getNearbyIndexes(n: Int, hIdx: Int) = {

        val boxCoords = reverseIndex(n, hIdx)

        //  -----------
        // | ↖ | ↑ | ↗ |
        // |---|---|---|
        // | ← | X | → |
        // |---|---|---|
        // | ↙ | ↓ | ↘ |
        //  -----------
        List((boxCoords._1 - 1, boxCoords._2 - 1), (boxCoords._1 - 1, boxCoords._2), (boxCoords._1 - 1, boxCoords._2 + 1), (boxCoords._1, boxCoords._2 + 1),
            (boxCoords._1 + 1, boxCoords._2 + 1), (boxCoords._1 + 1, boxCoords._2), (boxCoords._1 + 1, boxCoords._2 - 1), (boxCoords._1, boxCoords._2 - 1))
            .filter(coord => coord._1 >= 0 && coord._1 < n && coord._2 >= 0 && coord._2 < n)
            .map(coord => computeIndex(n, coord))
    }

    //    def main(args: Array[String]): Unit = {
    //
    //        val n = 8
    //
    //        //        (n - 1 to 0 by -1).foreach(row => {
    //        //
    //        //            print("%3d=> ".format(row))
    //        //
    //        //            (0 until n).foreach(col => print("| %3d ".format(computeIndex(n, (col, row)))))
    //        //
    //        //            println("|")
    //        //        })
    //        //
    //        //        (0 until (n * n)).foreach(hIdx => println("%d -> %s".format(hIdx, reverseIndex(n, hIdx))))
    //
    //        (0 until (n * n)).foreach(hIdx => println("%d -> %s".format(hIdx, getNearbyIndexes(n, hIdx))))
    //    }

    private def rotate(n: Int, x: RefInt, y: RefInt, rx: Int, ry: Int) {

        if (ry == 0) {
            if (rx == 1) {
                x.i = n - 1 - x.i
                y.i = n - 1 - y.i
            }

            //Swap x and y
            val t = x.i
            x.i = y.i
            y.i = t
        }
    }

    /**
      * A bit operation mapping from a Hilbert box coordinates ( (0,0)...(n-1, n-1)) to a Hilbert index (0...n-1)
      */
    private def mapToIndex(n: Int, x: RefInt, y: RefInt) = {

        var rx = false
        var ry = false
        var d = 0
        var s = n / 2
        var w = 0
        var z = 0

        while (s > 0) {

            rx = (x.i & s) > 0
            ry = (y.i & s) > 0

            w = if (rx) 1 else 0
            z = if (ry) 1 else 0

            d += (s * s * ((3 * w) ^ z))

            rotate(s, x, y, w, z)

            s /= 2
        }
        d
    }
}