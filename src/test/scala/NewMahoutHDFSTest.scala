import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.mahout.math.DenseMatrix
import org.apache.mahout.math.Matrix
import org.apache.mahout.math.MatrixSlice
import org.apache.mahout.math.MatrixUtils
import org.apache.mahout.math.SparseMatrix
import BIDMat.MatFunctions._
import BIDMat.Plotting._
import BIDMat.SciFunctions._
import BIDMat.Solvers._
import org.apache.mahout.math.Vector
import org.apache.mahout.math.SparseColumnMatrix
import org.apache.mahout.math.RandomAccessSparseVector
import scala.collection.mutable.ListBuffer
import org.apache.mahout.math.SparseRowMatrix
import NewMahoutHDFS._
import BIDMat.{Mat,SMat,FMat,SDMat,DMat,SparseMat}


object NewTestMahoutHDFS{
  
  //TODO: for now only tests for DMat
  def testWriteDenseMat(): Unit = 
  {
     println("Testing Writing Dense Mahout Matrix")
    var conf : Configuration = new Configuration()
    conf.set("fs.default.name", "hdfs://localhost:9000")
    var outFilePath : Path = new Path("DMat.txt")
    var dMat : FMat = 1\2\3 on 4\5\6 on 7\0\0
    write(dMat,conf,outFilePath)
    println("Printing DenseMatrix To Write")
    var  matrix : Matrix =  MatrixUtils.read(conf,outFilePath)
    0 until matrix.rowSize() foreach { r=>
      0 until matrix.columnSize() foreach { c=>
        print(matrix.get(r,c)+" ")
      }
      println()
    }
    println()
  }
  
  //TODO: for now only tests for SDMat
  def testWriteSparseMat(): Unit = 
  {
    var conf : Configuration = new Configuration()
    conf.set("fs.default.name", "hdfs://localhost:9000")
    var outFilePath : Path = new Path("SDMat.txt")
    //TODO: why is it printing random decimal numbers
    var sdMat : SDMat = sparse((1.1\0\3.0\2.0 on 4.6\0\0\1.2 on 0\2.5\1\5))
    write(sdMat,conf,outFilePath)
    var  matrix : Matrix =  MatrixUtils.read(conf,outFilePath)
    0 until matrix.rowSize() foreach { r=>
      0 until matrix.columnSize() foreach { c=>
        print(matrix.get(r,c)+" ")
      }
      println()
    }
    println()
  }
  
   //TODO: for now only tests for SDMat
  def testReadSparseMahoutMatrix(): Unit = 
  {
    println("Testing Reading Sparse Mahout Matrix")
    var conf : Configuration = new Configuration()
    conf.set("fs.default.name", "hdfs://localhost:9000")
    var inFilePath : Path = new Path("SDMat.txt")
    var sdMat : Mat = readSMat(conf,inFilePath)
    println(sdMat)
  }
  
  //TODO: for now only tests for DMat
  def testReadDenseMahoutMatrix(): Unit = 
  {
    println("Testing Reading Dense Mahout Matrix")
    var conf : Configuration = new Configuration()
    conf.set("fs.default.name", "hdfs://localhost:9000")
    var inFilePath : Path = new Path("DMat.txt")
    var dMat : Mat = readDMat(conf,inFilePath)
    println("Printing Read Out DMat")
    println(dMat)
  }
  
  def main(args: Array[String]): Unit = {
//    testWriteDenseMat()
//    testReadDenseMahoutMatrix()
    testWriteSparseMat()
    testReadSparseMahoutMatrix()
  }

}