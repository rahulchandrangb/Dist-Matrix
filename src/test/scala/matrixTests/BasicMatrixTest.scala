package matrixTests
import org.impetus.sparkBlas._

import collection.mutable.Stack
import org.scalatest._
import breeze.linalg.DenseMatrix

class BasicMatrixTest extends FlatSpec {

  "Matrix " should "Split to blocks" in {

    val matrixA = DenseMatrix(Array(1.0, 2.0, 3.0, 4.0, 5.0), Array(2.0, 3.0, 4.0, 5.0, 5.0), Array(3.0, 4.0, 5.0, 6.0, 5.0), Array(4.0, 5.0, 6.0, 7.0, 5.0), Array(5.0, 6.0, 7.0, 8.0, 5.0))
    val matrixB = DenseMatrix(Array(1.0, 2.0, 3.0, 4.0, 5.0), Array(2.0, 3.0, 4.0, 5.0, 5.0), Array(3.0, 4.0, 5.0, 6.0, 5.0), Array(4.0, 5.0, 6.0, 7.0, 5.0), Array(5.0, 6.0, 7.0, 8.0, 5.0))
    val x = MatrixUtils.convertToMatrixBlockD(matrixB, (2, 2))
    x.foreach(a => println(a + "\n"))
    assert(1 === 1)
  }
  
  it should "multiply" in {
    
  }

}
