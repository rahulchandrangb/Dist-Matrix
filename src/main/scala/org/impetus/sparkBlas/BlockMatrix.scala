package org.impetus.sparkBlas

import breeze.linalg.DenseMatrix
import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD

class Block(
  val rowIdx: Int,
  val colIdx: Int,
  val rowDiv: Int,
  val colDiv: Int,
  val data: DenseMatrix[Double]) {

  def transpose: Block = {
    val newData = data.t
    new Block(rowIdx, colIdx, rowDiv, colDiv, newData)
  }

  def multiply(other: Block): Block = {
    val newData = data * other.data
    new Block(rowIdx, colIdx, rowDiv, colDiv, newData)
  }
  override def toString: String = {
    data.toString
  }
}

class BlockMatrix(rowIdx: Int, colIdx: Int, data: RDD[Block]){
  override def toString:String = {
    println("Using to String?? Use with caution..")
    data.map(_.toString).collect.zipWithIndex.map(a => "Block:"+a._1+".\n"+a._2).mkString("\n\n\n")
  }
  
  
}

object Block {

}