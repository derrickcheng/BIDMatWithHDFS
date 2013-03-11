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
import BIDMat.{Mat,SMat,FMat,SDMat,DMat,IMat,SparseMat,DenseMat}
import Conversions._

//TODO: Add MatHDFS as a "SaveAsHDFS" to "MatFunctions"

object NewMahoutHDFS {
  //
  // Read DenseMats
  //
  def readIMat(conf : Configuration, inFilePath : Path) : IMat = {
    var fs : FileSystem = FileSystem.get(conf)
    var mMatrix : Matrix = MatrixUtils.read(conf,inFilePath)
    if (fs.exists(inFilePath))
    {
      mMatrix match {
        case dmMatrix : DenseMatrix => {
          return dmMatrix
        }
        case _ => {
          throw new RuntimeException("File needs to contain a DenseMatrix")
        }
      }
    }
    throw new RuntimeException("File does not exist ")
  }
  
  def readFMat(conf : Configuration, inFilePath : Path) : FMat = {
    var fs : FileSystem = FileSystem.get(conf)
    var mMatrix : Matrix = MatrixUtils.read(conf,inFilePath)
    if (fs.exists(inFilePath))
    {
      mMatrix match {
        case dmMatrix : DenseMatrix => {
          return dmMatrix
        }
        case _ => {
          throw new RuntimeException("File needs to contain a DenseMatrix")
        }
      }
    }
    throw new RuntimeException("File does not exist ")
  }
  
  def readDMat(conf : Configuration, inFilePath : Path) : DMat = {
    var fs : FileSystem = FileSystem.get(conf)
    var mMatrix : Matrix = MatrixUtils.read(conf,inFilePath)
    if (fs.exists(inFilePath))
    {
      mMatrix match {
        case dmMatrix : DenseMatrix => {
          return dmMatrix
        }
        case _ => {
          throw new RuntimeException("File needs to contain a DenseMatrix")
        }
      }
    }
    throw new RuntimeException("File does not exist ")
  }
  
  //
  // Write DenseMats
  //
  def writeDMat(conf : Configuration, inFilePath : Path) : DMat = {
    var fs : FileSystem = FileSystem.get(conf)
    var mMatrix : Matrix = MatrixUtils.read(conf,inFilePath)
    if (fs.exists(inFilePath))
    {
      mMatrix match {
        case dmMatrix : DenseMatrix => {
          return dmMatrix//denseMatrixToDenseMatM(dmMatrix,(nr:Int,nc:Int)=>DMat(nr,nc),(v:Double)=>v.toDouble)
        }
        case _ => {
          throw new RuntimeException("File needs to contain a DenseMatrix")
        }
      }
    }
    throw new RuntimeException("File does not exist ")
  }
  
  def writeFMat(conf : Configuration, inFilePath : Path) : FMat = {
    var fs : FileSystem = FileSystem.get(conf)
    var mMatrix : Matrix = MatrixUtils.read(conf,inFilePath)
    if (fs.exists(inFilePath))
    {
      mMatrix match {
        case dmMatrix : DenseMatrix => {
          return dmMatrix //denseMatrixToDenseMatM(dmMatrix,(nr:Int,nc:Int)=>FMat(nr,nc),(v:Double)=>v.toFloat)
        }
        case _ => {
          throw new RuntimeException("File needs to contain a DenseMatrix")
        }
      }
    }
    throw new RuntimeException("File does not exist ")
  }
  
  def writeIMat(conf : Configuration, inFilePath : Path) : IMat = {
    var fs : FileSystem = FileSystem.get(conf)
    var mMatrix : Matrix = MatrixUtils.read(conf,inFilePath)
    if (fs.exists(inFilePath))
    {
      mMatrix match {
        case dmMatrix : DenseMatrix => {
          return dmMatrix //denseMatrixToDenseMatM(dmMatrix,(nr:Int,nc:Int)=>IMat(nr,nc),(v:Double)=>v.toInt)
        }
        case _ => {
          throw new RuntimeException("File needs to contain a DenseMatrix")
        }
      }
    }
    throw new RuntimeException("File does not exist ")
  }
  
  // Read SparseMats
  def readSDMat(conf : Configuration, inFilePath : Path) : SDMat = {
    var fs : FileSystem = FileSystem.get(conf)
    var mMatrix : Matrix = MatrixUtils.read(conf,inFilePath)
    if (fs.exists(inFilePath))
    {
      mMatrix match {
        case srmMatrix : SparseRowMatrix => {
          return srmMatrix //sparseRowMatrixToSparseMatM(srmMatrix,(nr:Int,nc:Int,nnz:Int,ir:Array[Int],jc:Array[Int],data:Array[Double])=>SDMat(nr,nc,nnz,ir,jc,data),(v:Double)=>v.toDouble)
        }
        case _ => {
          throw new RuntimeException("File needs to contain a SparseRowMatrix")
        }
      }
    }
    throw new RuntimeException("File does not exist ")
    
  }
  
  def readSMat(conf : Configuration, inFilePath : Path) : SMat = {
    var fs : FileSystem = FileSystem.get(conf)
    var mMatrix : Matrix = MatrixUtils.read(conf,inFilePath)
    if (fs.exists(inFilePath))
    {
      mMatrix match {
        case srmMatrix : SparseRowMatrix => {
          return srmMatrix //sparseRowMatrixToSparseMatM(srmMatrix,(nr:Int,nc:Int,nnz:Int,ir:Array[Int],jc:Array[Int],data:Array[Float])=>SMat(nr,nc,nnz,ir,jc,data),(v:Double)=>v.toFloat)
        }
        case _ => {
          throw new RuntimeException("File needs to contain a SparseRowMatrix")
        }
      }
    }
    throw new RuntimeException("File does not exist ")
  }
  
  //TODO: currently writes almost everything
  def write(mat : Mat , conf : Configuration, outFilePath : Path): Unit = 
  {
    mat match{
      case sdMat : SDMat => {
        println("Writing Sparse SDMat")
        var dsmMatrix : Matrix = sdMat
        MatrixUtils.write(outFilePath,conf,dsmMatrix)
      }
      case sMat : SMat => {
        println("Writing Sparse SMat")
        var dsmMatrix : Matrix = sMat
        MatrixUtils.write(outFilePath,conf,dsmMatrix)
      }
      case dMat : DMat => {
        println("Writing Dense DMat")
        var dmMatrix : Matrix = dMat
        MatrixUtils.write(outFilePath,conf,dmMatrix)
      }
      case fMat : FMat => {
        println("Writing Dense FMat")
        var dmMatrix : Matrix = fMat
        MatrixUtils.write(outFilePath,conf,dmMatrix)
      }
      case iMat : IMat => {
        println("Writing Dense IMat")
        var dmMatrix : Matrix = iMat
        MatrixUtils.write(outFilePath,conf,dmMatrix)
      }
      case _ => {
        throw new RuntimeException("This type of Mat is not writable")
      }
    }
  }
  
  
  
//  //TODO: now always assumes input is DMat
//  def toDenseMatrix(dmMat : DMat) : Matrix = 
//  {
//    var numRows = dmMat.nrows
//    var numCols = dmMat.ncols
//    var dmMatrix : DenseMatrix = new DenseMatrix(numRows,numCols)
//    var r,c =0
//    while (r<numRows) { 
//      while (c<numCols) {
//        dmMatrix.setQuick(r,c,dmMat(r,c))
//        c+=1
//      }
//      r+=1
//    }
//    return dmMatrix
//  }
//  
//  //TODO: now always assumes input is SDMat
//  def toSparseMatrix(sdMat: SDMat) :Matrix =
//  {
//	var numRows = sdMat.nrows
//    var numCols = sdMat.ncols
//    var numNonZeros = sdMat.nnz
//    var ir = sdMat.ir0 // values are 0's based
//    var uncompressedjc = SparseMat.uncompressInds(sdMat.jc0, ir) // Not one's based
//    var data0 = sdMat.data
//    //TODO: Gotta Fix size(ir) == size(data) but size(jc) < size(data); Should be fixed
//    //TODO: gotta fix ones based indexing stuff...
//    var sdmMatrix : SparseMatrix = new SparseMatrix(numRows,numCols)
//	0 until numNonZeros foreach { i =>
//	  sdmMatrix.setQuick(ir(i)-Mat.ioneBased,uncompressedjc(i),data0(i))
//	}
//	return sdmMatrix
//  }
  
   //TODO: maybe add a T parameter that says Int, Float, Double?
//  def read(conf : Configuration, inFilePath : Path): Mat = 
//  {
//    var fs : FileSystem = FileSystem.get(conf)
//    var mMatrix : Matrix = MatrixUtils.read(conf,inFilePath)
//    if (fs.exists(inFilePath))
//    {
//      mMatrix match {
//        case dmMatrix : DenseMatrix => {
//          println("Dense Matrix")
//          return denseMatrixToDenseMatM(dmMatrix,(nr:Int,nc:Int)=>DMat(nr,nc),(v:Double)=>v.toDouble)
//        }
//        case srmMatrix : SparseRowMatrix => {
//          println("Sparse Row Matrix")
//          return sparseRowMatrixToSparseMatM(srmMatrix,(nr:Int,nc:Int,nnz:Int,ir:Array[Int],jc:Array[Int],data:Array[Double])=>SDMat(nr,nc,nnz,ir,jc,data),(v:Double)=>v.toDouble)
//        }
//        case _ => {
//          println("Default Case")
//        }
//      }
//    }
//    else 
//    {
//      println("File does not exist")
//    }
//    return null
//  }
  
  
}