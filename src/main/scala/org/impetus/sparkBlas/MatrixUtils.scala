package org.impetus.sparkBlas

import breeze.linalg.DenseMatrix
import scala.math._
import org.apache.spark.rdd.RDD
import breeze.linalg.DenseVector
import MatrixUtils._

object MatrixUtils {
  def multiplyDist(inp: RDD[Array[Double]], inp2: RDD[Array[Double]]) = {
    val indexedRdd = inp2.map { a =>
      var ind = 0 //cal also use a.toList.zipWithIndex
      a.map {
        b =>
          ind += 1
          (ind - 1, b)
      }
    }
    val flatAndGrouped = indexedRdd.flatMap(a => a).groupBy(_._1).map(_._2)
    val bothTogetherCart = inp.cartesian(flatAndGrouped)
    val calcElems = bothTogetherCart.map {
      tupl =>
        val rowArr = tupl._1
        val colArr = tupl._2
        val joinTog = rowArr.zip(colArr)
        val addedVal = joinTog.map {
          case (a, b) => (b._1, a * b._2)
        }
        val summedTogether = addedVal.reduce((a, b) => (b._1, a._2 + b._2))
        summedTogether
    }
    calcElems.map(v => v._2)
  }

  def convertToMatrixBlockD(inp: DenseMatrix[Double], split: (Int, Int)): Array[MatrixBlock[Double]] = {
    val m = split._1 //row split
    val n = split._2 //col split
    val numCalcs = ((inp.rows / m) + 1) * ((inp.cols / n) + 1)
    Iterator.iterate((Array[MatrixBlock[Double]](), 0, 0)) {
      case (x: Array[MatrixBlock[Double]], rowIdx: Int, colIdx: Int) =>
        val rowStartIndex = rowIdx * m
        val rowEndIndex = min(((rowIdx + 1) * m), inp.rows)
        val colStartIndex = colIdx * n
        val colEndIndex = min(((colIdx + 1) * n), inp.cols)
        val subM = inp(rowStartIndex until rowEndIndex, colStartIndex until colEndIndex).toDenseMatrix
        if ((rowIdx) * m >= inp.rows)
          (x, rowIdx, colIdx)
        else if ((colIdx + 1) * n >= inp.cols)
          (x :+ MatrixBlock(rowIdx * m, min((rowIdx + 1 * m), inp.rows - 1), colIdx * n, min((colIdx + 1) * n, inp.cols - 1), subM), rowIdx + 1, 0)
        else
          (x :+ MatrixBlock(rowIdx * m, min((rowIdx + 1 * m), inp.rows - 1), colIdx * n, min((colIdx + 1 * n), inp.cols - 1), subM), rowIdx, colIdx + 1)
    }.drop(numCalcs).next._1
  }

  def blockify(mat: RDD[(Int, DenseVector[Double])], rowBlockSize: Int, colBlockSize: Int) = {
    val splitRow = mat.flatMap {
      row =>
        row._2.toArray.toList.grouped(colBlockSize).toList.zipWithIndex.map {
          value => ((value._1, row._1), value._2)
        }
    }
    val rowBlockGroupedToCol = splitRow.groupBy {
      x =>
        ((x._1._2 / rowBlockSize), x._2)
    }

    val rddBlocks = rowBlockGroupedToCol.map {
      valSet =>
        val rowIdx = valSet._1._1
        val colIdx = valSet._1._2
        val dataArray = valSet._2.flatMap(x => x._1._1).toArray
        val dataMatrix = new DenseMatrix(rowBlockSize, colBlockSize, dataArray)
        new Block(rowIdx, colIdx, rowBlockSize, colBlockSize, dataMatrix)
    }
    
    val blkMatr = new BlockMatrix(rddBlocks,rowBlockSize,colBlockSize)
    blkMatr
  }
  
}