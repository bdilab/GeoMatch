/**
  * Copyright (c) 2019, The City University of New York and the University of Helsinki
  * All rights reserved.
  */

package org.cusp.bdi.gm.keyPart

/**
  * Class for representing a geometry key object
  */
case class GeomKey(partNum: Int) {}
class GeomKeySetOne(partNum: Int) extends GeomKey(partNum) {}
class GeomKeySetTwo(partNum: Int) extends GeomKey(partNum) {}
class GeomKeySetTwoSkip(partNum: Int) extends GeomKeySetTwo(partNum) {}