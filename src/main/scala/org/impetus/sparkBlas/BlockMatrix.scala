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
    new Block(rowIdx, colIdx, newData.rows, newData.cols, newData)
  }
  override def toString: String = {
    data.toString
  }
  def +(other: Block) = {
    new Block(rowIdx, colIdx, rowDiv, colDiv, (data + other.data))
  }
  def *(other: Block) = {
    Block(rowIdx, colIdx, rowDiv, colDiv, data :* other.data)
  }
  def -(other: Block) = {
    Block(rowIdx, colIdx, rowDiv, colDiv, data :- other.data)
  }

  def /(other: Block) = {
    Block(rowIdx, colIdx, rowDiv, colDiv, data :/ other.data)
  }

  def elemMultiply(con: Double) = {
    Block(rowIdx, colIdx, rowDiv, colDiv, data :* con)
  }
  def elemAdd(con: Double) = {
    Block(rowIdx, colIdx, rowDiv, colDiv, data :+ con)
  }
  def elemDiv(con: Double) = {
    Block(rowIdx, colIdx, rowDiv, colDiv, data :/ con)
  }
  def elemMinus(con: Double) = {
    Block(rowIdx, colIdx, rowDiv, colDiv, data :- con)
  }

  def getRowSize = data.rows
  def getColSize = data.cols

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
    val result = rowIndexed.join(colIndexed).map {
      comb =>
        val blk1 = comb._2._1
        val blk2 = comb._2._2
        val resultMat = blk1.data * blk2.data
        ((comb._1._1, comb._1._2), new Block(comb._1._1, comb._1._2, resultMat.rows, resultMat.cols, resultMat))
    }.persist.reduceByKey(_ + _).map(_._2)
    BlockMatrix(result, rowBlocksNum, otherMat.colBlocksNum)

  }

  def elemMultiply(constnt: Double) = {
    val broadCastConstant = data.context.broadcast(constnt)
    val newData = this.data.map { blk =>
      blk.elemMultiply(broadCastConstant.value)
    }
    BlockMatrix(newData, rowBlockSize, colBlockSize)
  }

  def elemAdd(constnt: Double) = {
    val broadCastConstant = data.context.broadcast(constnt)
    val newData = this.data.map { blk =>
      blk.elemAdd(broadCastConstant.value)
    }
    BlockMatrix(newData, rowBlockSize, colBlockSize)
  }

  def elemMinus(constnt: Double) = {
    val broadCastConstant = data.context.broadcast(constnt)
    val newData = this.data.map { blk =>
      blk.elemMinus(broadCastConstant.value)
    }
    BlockMatrix(newData, rowBlockSize, colBlockSize)
  }

  def elemDivide(constnt: Double) = {
    val broadCastConstant = data.context.broadcast(constnt)
    val newData = this.data.map { blk =>
      blk.elemDiv(broadCastConstant.value)
    }
    BlockMatrix(newData, rowBlockSize, colBlockSize)
  }

  def /(other: BlockMatrix) = {
    //TODO: (Rahul  Chandran)...Is group By/ sort By key required??
    val newData = this.data.zip(other.data).map {
      blk =>
        blk._1 / blk._2
    }
    BlockMatrix(newData, rowBlockSize, colBlockSize)
  }

  def *(other: BlockMatrix) = { //Scalar multiplication..
    //TODO: (Rahul  Chandran)...Is group By/ sort By key required??
    val newData = this.data.zip(other.data).map {
      blk =>
        blk._1 * blk._2
    }
    BlockMatrix(newData, rowBlockSize, colBlockSize)
  }

  def +(other: BlockMatrix) = {
    val newData = this.data.zip(other.data).map {
      blk =>
        blk._1 + blk._2
    }
    BlockMatrix(newData, rowBlockSize, colBlockSize)
  }

  def -(other: BlockMatrix) = {
    val newData = this.data.zip(other.data).map {
      blk =>
        blk._1 - blk._2
    }
    BlockMatrix(newData, rowBlockSize, colBlockSize)
  }

}
