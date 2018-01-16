package com.example.analytics.common

import com.example.commons.collection.ListImplicits.ListHelper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.util._

/**
  * IMPLICITS: ANALYTICS
  */
object AggregationImplicits {


  implicit class DataFrameHelper(val ds: Dataset[Row]) {

    val isListDefined = (x: List[Option[Any]]) => x.map(_.isDefined).foldLeft(true)(_ && _)
    val aggregationFunction = (df: DataFrame, mapExpr: Map[String, String]) => df.agg(mapExpr).collect.last.toSeq.toList

    private def calAggResults[T] (mapExprsTry: Try[Map[String, String]]) = {
      mapExprsTry.flatMap { mapExprs =>
        if (mapExprs.nonEmpty) Success(aggregationFunction(ds, mapExprs).mapToType[T])
        else Success(List())
      }
    }


    /**
      *
      * @param columnOpt  List Of columns Option(List("C1","C2","C3","C4"))
      * @param aggListOpt List of aggregations Ex: Option(List("sum","mean","min","max"))
      * @tparam T Long,Int,Double,String
      * @return returns a list of results of genric Type T, List[T] (use string for complex types)
      */

    def getAggFromList[T](columnOpt: Option[List[String]], aggListOpt: Option[List[String]], defaultAgg:String = "sum" ,columnList:Option[List[String]] = None): Try[List[T]] = {
      val colList        =           columnList.getOrElse(ds.columns.toList)
      val columnIndexOpt =           columnOpt.map{x => x.map(colList.indexOf(_))}

      getAggFromListIndexes[T](columnIndexOpt, aggListOpt)
    }


    /**
      *
      * @param columnIndexOpt List Of collumn Indexes Ex: List(2,5,6,8)
      * @param aggListOpt     List of aggregations Ex: List("sum","mean","min","max")
      * @tparam T T as {Long,Int,Double,String}
      * @return returns a list of results of genric of Type List[T] (use string for varying types in List)
      */

    def getAggFromListIndexes[T](columnIndexOpt: Option[List[Int]], aggListOpt: Option[List[String]],
                           defaultAgg:String = "sum"): Try[List[T]]  = {

      val colList = ds.columns.toList

      lazy val aggList    =          aggListOpt.getOrElse(List(defaultAgg))
      lazy val column     =          columnIndexOpt.getOrElse(List())
      lazy val coumnNames =          column.map(colList(_))

      val onlyAggListIsDefined =     columnIndexOpt.isEmpty && aggListOpt.isDefined
      val onlyColListIsDefined =     columnIndexOpt.isDefined && aggListOpt.isEmpty
      val areBothInputsDefined =     columnIndexOpt.isDefined && aggListOpt.isDefined

      var mapExprs: Try[Map[String, String]] = Success(Map())

      if (onlyAggListIsDefined)      mapExprs = Success(colList.take(aggList.length).zip(aggList).toMap)
      else if (areBothInputsDefined) mapExprs = getAggFrom[T](coumnNames,aggList,colList)
      else if (onlyColListIsDefined) mapExprs = Success(coumnNames.map(x =>(x,defaultAgg)).toMap)
      calAggResults[T](mapExprs)
    }

    private def getAggFrom[T](colNameList:List[String], aggList:List[String],colList:List[String])
    : Try[Map[String,String]] = {
      val isOfSameSize = colNameList.length != aggList.length
      if (isOfSameSize){
        val mapExprs = colNameList.zip(aggList).toMap
          Success(mapExprs)
      } else {
          Failure(new IllegalArgumentException(s"Length should be same for Inputs"
          +s"$colNameList and $aggList"))
      }
    }

  }
}
