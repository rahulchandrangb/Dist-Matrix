package org.rahul.blas

import org.apache.spark.rdd.RDD

case class DistMatrix(numRow:Int,numCols:Int,data:Array[Double]) extends Serializable{
  def asArraySet = {
    
  }  
}
