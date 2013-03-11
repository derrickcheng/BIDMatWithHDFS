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
import BIDMat.{Mat,SMat,FMat,SDMat,DMat,SparseMat}

//TODO: Add MatHDFS as a "SaveAsHDFS" to "MatFunctions"

object MahoutHDFS {
  
  // Read DenseMats
  def readSMat(conf : Configuration, inFilePath : Path) : SMat = {
    return null
  }
  
  def readFMat(conf : Configuration, inFilePath : Path) : FMat = {
    return null
  }
  
  def readDMat(conf : Configuration, inFilePath : Path) : SMat = {
    return null
  }
  
  def readIMat(conf : Configuration, inFilePath : Path) : FMat = {
    return null
  }
  
  // Write DenseMats
  def writeSMat(conf : Configuration, inFilePath : Path) : SMat = {
    return null
  }
  
  def writeFMat(conf : Configuration, inFilePath : Path) : FMat = {
    return null
  }
  
  def writeDMat(conf : Configuration, inFilePath : Path) : SMat = {
    return null
  }
  
  def writeIMat(conf : Configuration, inFilePath : Path) : FMat = {
    return null
  }
  
  //TODO: maybe add a T parameter that says Int, Float, Double?
  def read(conf : Configuration, inFilePath : Path): Mat = 
  {
    var fs : FileSystem = FileSystem.get(conf)
    var mMatrix : Matrix = MatrixUtils.read(conf,inFilePath)
    if (fs.exists(inFilePath))
    {
      mMatrix match {
        case dmMatrix : DenseMatrix => {
          println("Dense Matrix")
          return getDMat(dmMatrix)
        }
        case srmMatrix : SparseRowMatrix => {
          println("Sparse Row Matrix")
          return getSDMat(srmMatrix)
        }
        case _ => {
          println("Default Case")
        }
      }
    }
    else 
    {
      println("File does not exist")
    }
    return null
  }
  
  //TODO: currently only takes in SDMat and DMats
  def write(mat : Mat , conf : Configuration, outFilePath : Path): Unit = 
  {
    mat match{
      case dsMat : SDMat => {
        println("Writing Sparse DMat")
        var dsmMatrix : Matrix = toSparseMatrix(dsMat)
        MatrixUtils.write(outFilePath,conf,dsmMatrix)
      }
      case dMat : DMat => {
        println("Writing DMat")
        var dmMatrix : Matrix = toDenseMatrix(dMat)
        MatrixUtils.write(outFilePath,conf,dmMatrix)
      }
      case _ => {
        println("Default Case, can't write")
//        throw new RuntimeException("mat must be DMat or SDMat")

      }
    }
  }
  
  def getDMat(dmMatrix : DenseMatrix) : DMat = 
  {
    var numRows = dmMatrix.numRows()
    var numCols = dmMatrix.numCols()
    var dMat : DMat = DMat(numRows,numCols)
    val data = dMat.data
    0 until numCols foreach { c =>
      0 until numRows foreach { r =>
        data(r+c*numRows)=dmMatrix.getQuick(r,c)
      }
    }
    return dMat
  }
  
  
    //TODO: Type this to get any type of sparseMatrix not just SDMat's
  def getSDMat(msrMatrix: SparseRowMatrix) :SDMat =
  {
    var nrows = msrMatrix.numRows()
    var ncols = msrMatrix.numCols()
    var nnz = 0
    // Figure out the number of non-zeros
    var ct=0
    while (ct<nrows)
    {
      var msrVector : Vector = msrMatrix.viewRow(ct)
      var values : java.util.Iterator[Vector.Element] = msrVector.iterateNonZero()
      while (values.hasNext())
      {
    	var vNZ : Vector.Element =  values.next()
	    var vNZvalue : Double= vNZ.get()
	    if (vNZvalue>0)
	    {
	    	nnz+=1
	    }
      }
      ct+=1
    }
    var sdMat : SDMat = new SDMat(nrows, ncols, nnz, new Array[Int](nnz), new Array[Int](ncols+1), new Array[Double](nnz))
    var cols = new Array[Int](nnz)
    var vals = new Array[Double](nnz)
    var r = 0
    var nnzCtr = 0
    while (r<nrows)
    {
      var msrVector : Vector = msrMatrix.viewRow(r)
      var nonZeros : java.util.Iterator[Vector.Element] = msrVector.iterateNonZero()
      while (nonZeros.hasNext())
      {
	    var vNZ : Vector.Element =  nonZeros.next()
	    var vNZindex : Int = vNZ.index()
	    var vNZvalue : Double= vNZ.get()
	    //TODO : why is nonZeros.next not actually returning non-zeros?
	    if (vNZvalue>0)
	    {
	      // TODO: ir is supposed to be one's based?
	    	sdMat.ir(nnzCtr)=r+Mat.ioneBased
	    	cols(nnzCtr)=vNZindex
	    	vals(nnzCtr)=vNZvalue
	    	nnzCtr+=1
	    }
      }
      r+=1
    }
    val isort = Mat.ilexsort2(cols,sdMat.ir)
    var i = 0; while (i < cols.length) {sdMat.data(i) = vals(isort(i)); i+=1}
    SparseMat.compressInds(cols,ncols,sdMat.jc,nnz) // sets sdMat's jc
	return sdMat
  }
  
  //TODO: now always assumes input is DMat
  def toDenseMatrix(dmMat : DMat) : Matrix = 
  {
    var numRows = dmMat.nrows
    var numCols = dmMat.ncols
    var dmMatrix : DenseMatrix = new DenseMatrix(numRows,numCols)
    var r,c =0
    while (r<numRows) { 
      while (c<numCols) {
        dmMatrix.setQuick(r,c,dmMat(r,c))
        c+=1
      }
      r+=1
    }
    return dmMatrix
  }
  
  //TODO: now always assumes input is SDMat
  def toSparseMatrix(sdMat: SDMat) :Matrix =
  {
	var numRows = sdMat.nrows
    var numCols = sdMat.ncols
    var numNonZeros = sdMat.nnz
    var ir = sdMat.ir0 // values are 0's based
    var uncompressedjc = SparseMat.uncompressInds(sdMat.jc0, ir) // Not one's based
    var data0 = sdMat.data
    //TODO: Gotta Fix size(ir) == size(data) but size(jc) < size(data); Should be fixed
    //TODO: gotta fix ones based indexing stuff...
    var sdmMatrix : SparseMatrix = new SparseMatrix(numRows,numCols)
	0 until numNonZeros foreach { i =>
	  sdmMatrix.setQuick(ir(i)-Mat.ioneBased,uncompressedjc(i),data0(i))
	}
	return sdmMatrix
  }
  
}