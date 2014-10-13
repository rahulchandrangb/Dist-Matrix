package org.impetus.sparkBlas

import breeze.linalg.DenseMatrix
import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD

class Block(
  val rowIdx: Int,
  val colIdx: Int,
  val rowDiv: Int,
  val colDiv: Int,
  val data: DenseMatrix[Double]) extends Serializable {
  def transpose: Block = {
    val newData = data.t
    new Block( colIdx,rowIdx, colDiv, rowDiv, newData)
  }

  def multiply(other: Block): Block = {
    val newData = data * other.data
    new Block(rowIdx, colIdx, rowDiv, colDiv, newData)
  }
  override def toString: String = {
    data.toString
  }
}

class BlockMatrix(data: RDD[Block],val rowBlockSize:Int,val colBlockSize:Int) extends Serializable {
 
  override def toString:String = {
    println("Using toString?? Use with caution..")
    data.map(_.toString).collect.zipWithIndex.map(a => "Block:"+a._1+".\n"+a._2).mkString("\n\n\n")
  }
  
  def transpose={
    val newData = data.map(_.transpose)
    
    
  }
  
}
