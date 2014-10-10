package org.impetus.sparkBlas

import breeze.linalg.DenseMatrix
import breeze.linalg.DenseVector
import scala.specialized

case class MatrixBlock[@specialized(Double, Int, Float, Long) T](val rowStart:Int,val rowEnd:Int,val colStart:Int,val colEnd:Int,val data:DenseMatrix[T]) extends Serializable{
  override  def  toString():String = {
		  data.toString
  }
}
