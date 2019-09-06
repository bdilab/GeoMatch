/**
  * Copyright (c) 2019, The City University of New York and the University of Helsinki
  * All rights reserved.
  */

package org.cusp.bdi.gm.keyPart

import org.apache.spark.Partitioner

/**
  * Returns the partition's number for the specified index
  */
case class GMPartitioner(_numPartitions: Int) extends Partitioner {

    override def numPartitions = _numPartitions

    override def getPartition(key: Any): Int =
        key match {
            case x: GeomKey => x.partNum
        }
}