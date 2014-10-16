package org.impetus.sparkBlas

import breeze.linalg.DenseMatrix
import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.SparkContext._

case class Block(
  val rowIdx: Int,
  val colIdx: Int,
  val rowDiv: Int,
  val colDiv: Int,
  val data: DenseMatrix[Double]) extends Serializable {

  def transpose: Block = {
    val newData = data.t
    new Block(colIdx, rowIdx, colDiv, rowDiv, newData)
  }

  def multiply(other: Block): Block = {
    val newData = data * other.data
    new Block(rowIdx, colIdx, rowDiv, colDiv, newData)
  }
  override def toString: String = {
    data.toString
  }

}

case class BlockMatrix(val data: RDD[Block], val rowBlockSize: Int, val colBlockSize: Int) extends Serializable {

  private lazy val rowBlocksNum = data.filter(_.colIdx == 0).count.toInt
  private lazy val colBlocksNum = data.filter(_.rowIdx == 0).count.toInt

  override def toString: String = {
    println("Using toString?? Use with caution..")
    data.map(_.toString).collect.zipWithIndex.map(a => "Block:" + a._1 + ".\n" + a._2).mkString("\n\n\n")
  }

  def transpose: BlockMatrix = {
    val newDataIndexed = data.map(_.transpose).map {
      blk =>
        ((blk.rowIdx, blk.colIdx), blk)
    }
    val newData = newDataIndexed.sortByKey(true).map(_._2)
    BlockMatrix(newData, colBlockSize, rowBlockSize)
  }

  def multiply(otherMat: BlockMatrix) = {
    val thisRowBlockSize = data.context.broadcast(rowBlocksNum)
    val thisColBlockSize = data.context.broadcast(colBlocksNum)
    val otherColBlockSize = data.context.broadcast(otherMat.colBlockSize)

    val rowIndexed = data.flatMap {
      r =>
        val nSplitNum = otherColBlockSize.value
        val kSplitNum = thisColBlockSize.value

        val array = Array.ofDim[((Int, Int, Int), Block)](nSplitNum)

        for (i <- 0 until nSplitNum) {
          val seq = r.rowIdx * nSplitNum * kSplitNum + i * kSplitNum + r.colIdx
          array(i) = ((r.rowIdx, i, seq), r)
        }
        array
    }

    val colIndexed = otherMat.data.flatMap {
      c =>
        val mSplitNum = thisRowBlockSize.value
        val kSplitNum = thisColBlockSize.value
        val nSplitNum = otherColBlockSize.value

        val array = Array.ofDim[((Int, Int, Int), Block)](mSplitNum)

        for (j <- 0 until mSplitNum) {
          val seq = j * nSplitNum * kSplitNum + c.colIdx * kSplitNum + c.colIdx
          array(j) = ((j, c.colIdx, seq), c)
        }
        array
    }

    if (colBlocksNum != 1) {
      val result = rowIndexed.join(colIndexed).map {
        comb =>
        	val blk1 = comb._2._1
        	val blk2 = comb._2._2
        	
        	
      }

      /*
      
        .mapPartitions({iter =>
        iter.map{ t =>
          val b1 = t._2._1.asInstanceOf[BDM[Double]]
          val b2 = t._2._2.asInstanceOf[BDM[Double]]
          val c = (b1 * b2).asInstanceOf[BDM[Double]]
          (new BlockID(t._1.row, t._1.column), c)
        }}).partitionBy(partitioner).persist().reduceByKey( _ + _ )
      new BlockMatrix(result, this.numRows(), other.numCols(), mSplitNum, nSplitNum)
      
      */
    } else {
      /*
      val result = rowIndexed.join(colIndexed)
        .mapValues(t => (t._1.asInstanceOf[BDM[Double]] * t._2.asInstanceOf[BDM[Double]])
        .asInstanceOf[BDM[Double]])
      new BlockMatrix(result, this.numRows(), other.numCols(), mSplitNum, nSplitNum)
      * */

    }

  }
}
