/**
  * Copyright (c) 2019, The City University of New York and the University of Helsinki
  * All rights reserved.
  */

package org.cusp.bdi.util

import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

/**
  * Helper class for parsing and retrieving command line arguments. The class parses the args array based on the argument list.
  * Error message are printed using the info in the list versus what is provided in the args array
  *
  */
case class CLArgsParser(args: Array[String], lstArgInfo: List[(String, String, Any, String)]) extends Serializable {

    private val SPACER = "       "
    private val mapProgramArg = HashMap[String, Any]()
    private var mapCLArgs: Map[String, String] = null

    def updateParamValue(paramInfo: (String, String, Any, String), newVal: Any) = {

        mapProgramArg.update(paramInfo._1.toLowerCase(), newVal)

        this
    }

    def getParamValueChar(paramInfo: (String, String, Any, String)) = getParamValueString(paramInfo).charAt(0)
    def getParamValueDouble(paramInfo: (String, String, Any, String)) = getParamValueString(paramInfo).toDouble
    def getParamValueFloat(paramInfo: (String, String, Any, String)) = getParamValueString(paramInfo).toFloat
    def getParamValueInt(paramInfo: (String, String, Any, String)) = getParamValueString(paramInfo).toInt
    def getParamValueString(paramInfo: (String, String, Any, String)) = mapProgramArg.get(paramInfo._1.toLowerCase()).get.toString()
    def getParamValueBoolean(paramInfo: (String, String, Any, String)) = getParamValueString(paramInfo).toLowerCase() match {

        case "t" | "true" | "y" => true
        case _ => false
    }

    buildArgMap()

    /**
      * @param commandLineArgs, the array of arguments
      * @param paramInfo, a list of tuples consisting of
      * 					(parameter name, Data type, description, default value (null means that the argument is required))
      */
    def buildArgMap() {

        try {

            var argsArr = args

            if (argsArr == null)
                argsArr = Array("")

            var missingArg = false

            val lstArgs = ListBuffer[String](args: _*)

            var i = 1
            while (i < lstArgs.size) {

                //                if (lstArgs(i).equals(lstArgs(i - 1)))
                if (lstArgs(i).equals(" ") && !lstArgs(i - 1).startsWith("-") ||
                    lstArgs(i).length() == 0 && !lstArgs(i - 1).startsWith("-") ||
                    lstArgs(i).equals(" ") && lstArgs(i - 1).equals(" ") || lstArgs(i).length() == 0 && lstArgs(i - 1).length() == 0)
                    lstArgs.remove(i)
                else
                    i += 1
            }

            mapCLArgs = lstArgs.grouped(2).map(arr => arr(0).toLowerCase() -> arr(1)).toMap

            lstArgInfo.foreach(t => {

                val (key, valType, valDefault) = (t._1.toLowerCase(), t._2.toLowerCase(), t._3)

                var paramVal = mapCLArgs.get("-" + key)

                if (paramVal == None) {

                    mapProgramArg += key -> valDefault

                    if (valDefault == null)
                        missingArg = true
                }
                else
                    valType match {
                        case "char" =>
                            val c = paramVal.get.trim()

                            if (c.length() == 1)
                                mapProgramArg += key -> c
                            else {
                                mapProgramArg += key -> (paramVal.get + "<-Err")
                                missingArg = true
                            }
                        case "int" =>
                            try { mapProgramArg += key -> paramVal.get.trim().toInt }
                            catch {
                                case _: Exception =>
                                    mapProgramArg += key -> (paramVal.get + "<-Err")
                                    missingArg = true
                            }
                        case "double" =>
                            try { mapProgramArg += key -> paramVal.get.trim().toDouble }
                            catch {
                                case _: Exception =>
                                    mapProgramArg += key -> (paramVal.get + "<-Err")
                                    missingArg = true
                            }
                        case "bollean" =>
                            try { mapProgramArg += key -> paramVal.get.trim().toBoolean }
                            catch {
                                case _: Exception =>
                                    mapProgramArg += key -> (paramVal.get + "<-Err")
                                    missingArg = true
                            }
                        case _: String =>
                            mapProgramArg += key -> paramVal.get.trim()
                        case _ =>
                            throw new Exception(buildUsageString())
                    }
            })

            if (mapProgramArg.size == 0 || missingArg)
                throw new Exception(buildUsageString())
        }
        catch {
            case ex: Exception => {

                ex.printStackTrace()

                val sb = StringBuilder.newBuilder

                sb.append(ex.toString())
                    .append('\n')
                    .append("Number of args received ")
                    .append(args.length)
                    .append(" out of the expected ")
                    .append(lstArgInfo.size * 2)
                    .append('\n')

                args.foreach(x => sb.append(x).append(" "))

                throw new Exception(sb.toString())
            }
        }
    }

    def buildUsageString() = {

        val sb = StringBuilder.newBuilder

        sb.append("Missing one or more required parameters or incorrect value types.")
            .append('\n')
            .append("Usage: ")
            .append('\n')
            .append(toString())
            .toString()
    }

    def toString(clazz: AnyRef) = {

        var sb = StringBuilder.newBuilder

        if (clazz != null)
            sb.append(clazz.getClass().getName)
                .append("_")

        sb.append(SPACER)

        lstArgInfo.foreach(t => {

            val (key, valType, valDefault, valDesc) = (t._1, t._2.toLowerCase(), t._3, t._4)

            sb.append("-")
                .append(key)
                .append(", Type: ")
                .append(valType)
                .append(", Required: ")

            if (valDefault == null)
                sb.append("Yes")
            else
                sb.append("No")

            sb.append(" (Value ")

            if (mapCLArgs.get("-" + key.toLowerCase()) == None)
                if (valDefault == null)
                    sb.append("NOT Set")
                        .append(")")
                else
                    sb.append("defaulted to: ")
                        .append(t._3)
                        .append(")")
            else
                sb.append("set to: ")
                    .append(mapProgramArg.get(key.toLowerCase()).get)

            sb.append(", Desc: ")
                .append(valDesc)
                .append('\n')
                .append(SPACER)
        })

        sb.delete(sb.length - SPACER.length() - 1, sb.length)
            .toString()
    }

    override def toString() =
        this.toString(null)
}

object Test_CLArgsParser {

    def main(args: Array[String]): Unit = {

        val lstReqParamInfo = List[(String, String, Any, String)](("local", "Boolean", false, "(T=local, F=cluster)"),
            ("debug", "Boolean", false, "(T=show_debug, F=no_debug)"),
            ("outDir", "String", null, "File location to write benchmark results"),
            ("firstSet", "String", null, "First data set input file path (LION Streets)"),
            ("firstSetObj", "String", null, "The object type indicator (e.g. LION_LineString, TPEP_Point ...)"),
            ("secondSet", "String", null, "Second data set input file path (Bus, TPEP, Yellow)"),
            ("secondSetObj", "String", null, "The object type indicator (e.g. LION_LineString, TPEP_Point ...)"),
            ("queryType", "String", null, "The query type (e.g. distance, kNN, range)"),
            ("hilbertN", "Int", 256, "The size of the hilbert curve (i.e. n)"),
            ("errorRange", "Double", 150, "Value by which to expand each MBR prior to adding it to the RTree"),
            ("matchCount", "Int", 3, "Number of matched geometries to keept (i.e. # points per streeet)"),
            ("matchDist", "Double", 150, "Maximum distance after which the match is discarded"),
            ("searchGridMinX", "Int", -1, "Search grid's minimum X"),
            ("searchGridMinY", "Int", -1, "Search grid's minimum Y"),
            ("searchGridMaxX", "Int", -1, "Search grid's maximum X"),
            ("searchGridMaxY", "Int", -1, "Search grid's maximum Y"))

        println(CLArgsParser(args, lstReqParamInfo))
    }
}