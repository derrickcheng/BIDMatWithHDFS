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

object Conversions {
  
  implicit def DenseMatrixToDMat(dmMatrix : DenseMatrix) :DMat = 
  {
     return denseMatrixToDenseMatM(dmMatrix,(nr:Int,nc:Int)=>DMat(nr,nc),(v:Double)=>v.toDouble)
  }
  
  implicit def DenseMatrixToIMat(dmMatrix : DenseMatrix) :IMat = 
  {
     return denseMatrixToDenseMatM(dmMatrix,(nr:Int,nc:Int)=>IMat(nr,nc),(v:Double)=>v.toInt)
  }
  
  implicit def DenseMatrixToFMat(dmMatrix : DenseMatrix) :FMat = 
  {
     return denseMatrixToDenseMatM(dmMatrix,(nr:Int,nc:Int)=>FMat(nr,nc),(v:Double)=>v.toFloat)
  }
  
  implicit def SparseMatrixToSDMat(srmMatrix : SparseRowMatrix) :SDMat = 
  {
     return sparseRowMatrixToSparseMatM(srmMatrix,(nr:Int,nc:Int,nnz:Int,ir:Array[Int],jc:Array[Int],data:Array[Double])=>SDMat(nr,nc,nnz,ir,jc,data),(v:Double)=>v.toDouble)
  }
  
  implicit def SparseMatrixToSMat(srmMatrix : SparseRowMatrix) : SMat =
  {
    return sparseRowMatrixToSparseMatM(srmMatrix,(nr:Int,nc:Int,nnz:Int,ir:Array[Int],jc:Array[Int],data:Array[Float])=>SMat(nr,nc,nnz,ir,jc,data),(v:Double)=>v.toFloat)
  }
  
  implicit def DMatToDenseMatrix(dMat : DMat)  : DenseMatrix = 
  {
     return denseMatMToDenseMatrix(dMat,(v : Double)=>v.toDouble)
  }
  
  implicit def FMatToDenseMatrix(dMat : FMat)  : DenseMatrix = 
  {
     return denseMatMToDenseMatrix(dMat,(v : Float)=>v.toDouble)
  }
  
  implicit def IMatToDenseMatrix(dMat : IMat)  : DenseMatrix = 
  {
     return denseMatMToDenseMatrix(dMat,(v : Int)=>v.toDouble)
  }
  
  implicit def SMatToSparseMatrix(sMat : SMat)  : SparseMatrix = 
  {
     return sparseMatMToSparseMatrix(sMat,(v : Float)=>v.toDouble)
  }
  
  implicit def SDMatToSparseMatrix(sdMat : SDMat)  : SparseMatrix = 
  {
     return sparseMatMToSparseMatrix(sdMat,(v : Double)=>v.toDouble)
  }
 
  /**
   * 
   * HELPERS FOR Conversions
   * 
   */
  //TODO: Gotta make sure this works
  def denseMatMToDenseMatrix[T, M <:DenseMat[T]](dmMat : M, convert : (T)=>Double) : DenseMatrix = 
  {
    var numRows = dmMat.nrows
    var numCols = dmMat.ncols
    var dmMatrix : DenseMatrix = new DenseMatrix(numRows,numCols)
    var r,c =0
    while (r<numRows) { 
      c = 0
      while (c<numCols) {
        var v :T = dmMat(r,c)
        dmMatrix.setQuick(r,c,convert(v))
        c+=1
      }
      r+=1
    }
    return dmMatrix
  }
  
  //TODO: Gotta make sure this works for inheritence
  def sparseMatMToSparseMatrix[T,M <: SparseMat[T]](sparseMat : M, convert : (T) => Double) :SparseMatrix =
  {
	var numRows = sparseMat.nrows
    var numCols = sparseMat.ncols
    var numNonZeros = sparseMat.nnz
    var ir = sparseMat.ir // values are 0's based
    var uncompressedjc = SparseMat.uncompressInds(sparseMat.jc, ir) // Not one's based
    var data = sparseMat.data
    var sdmMatrix : SparseMatrix = new SparseMatrix(numRows,numCols)
	0 until numNonZeros foreach { i =>
	  sdmMatrix.setQuick(ir(i)-Mat.ioneBased,uncompressedjc(i),convert(data(i)))
	}
	return sdmMatrix
  }
  
  def denseMatrixToDenseMatM[T,M <: DenseMat[T]](dmMatrix : DenseMatrix, constructMat : (Int,Int) => M, convertType : (Double)=>T): M = 
  {
    var numRows = dmMatrix.numRows()
    var numCols = dmMatrix.numCols()
    var mat : M = constructMat(numRows,numCols)
    val data : Array[T] = mat.data
    0 until numCols foreach { c =>
      0 until numRows foreach { r =>
        data(r+c*numRows)=convertType(dmMatrix.getQuick(r,c))
      }
    }
    return mat
  }
  
  def sparseRowMatrixToSparseMatM[T : Manifest, M <: SparseMat[T]](msrMatrix: SparseRowMatrix, constructMat : (Int,Int,Int,Array[Int],Array[Int],Array[T]) => M, convertType : (Double)=>T) :M =
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
    var mat : M = constructMat(nrows, ncols, nnz, new Array[Int](nnz), new Array[Int](ncols+1), new Array[T](nnz))
    var cols = new Array[Int](nnz)
    var vals : Array[T] = new Array[T](nnz)
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
	    	mat.ir(nnzCtr)=r+Mat.ioneBased
	    	cols(nnzCtr)=vNZindex
	    	vals(nnzCtr)=convertType(vNZvalue)
	    	nnzCtr+=1
	    }
      }
      r+=1
    }
    val isort = Mat.ilexsort2(cols,mat.ir)
    var i = 0; while (i < cols.length) {mat.data(i) = vals(isort(i)); i+=1}
    SparseMat.compressInds(cols,ncols,mat.jc,nnz) // sets sdMat's jc
	return mat
  }
  
  
    //TODO: Remove deprecated
//  def denseMatrixToDenseMatOfTypeT[T](dmMatrix : DenseMatrix, constructMat : (Int,Int) => DenseMat[T], convertType : (Double)=>T): DenseMat[T] = 
//  {
//    var numRows = dmMatrix.numRows()
//    var numCols = dmMatrix.numCols()
//    var mat : DenseMat[T] = constructMat(numRows,numCols)
//    val data : Array[T] = mat.data
//    0 until numCols foreach { c =>
//      0 until numRows foreach { r =>
//        data(r+c*numRows)=convertType(dmMatrix.getQuick(r,c))
//      }
//    }
//    return mat
//  }
  
  //TODO: Remove deprecated
//  def sparseRowMatrixToSparseMatOfTypeT[T : Manifest](msrMatrix: SparseRowMatrix, constructMat : (Int,Int,Int,Array[Int],Array[Int],Array[T]) => SparseMat[T], convertType : (Double)=>T) :SparseMat[T] =
//  {
//    var nrows = msrMatrix.numRows()
//    var ncols = msrMatrix.numCols()
//    var nnz = 0
//    // Figure out the number of non-zeros
//    var ct=0
//    while (ct<nrows)
//    {
//      var msrVector : Vector = msrMatrix.viewRow(ct)
//      var values : java.util.Iterator[Vector.Element] = msrVector.iterateNonZero()
//      while (values.hasNext())
//      {
//    	var vNZ : Vector.Element =  values.next()
//	    var vNZvalue : Double= vNZ.get()
//	    if (vNZvalue>0)
//	    {
//	    	nnz+=1
//	    }
//      }
//      ct+=1
//    }
//    var mat : SparseMat[T] = constructMat(nrows, ncols, nnz, new Array[Int](nnz), new Array[Int](ncols+1), new Array[T](nnz))
//    var cols = new Array[Int](nnz)
//    var vals : Array[T] = new Array[T](nnz)
//    var r = 0
//    var nnzCtr = 0
//    while (r<nrows)
//    {
//      var msrVector : Vector = msrMatrix.viewRow(r)
//      var nonZeros : java.util.Iterator[Vector.Element] = msrVector.iterateNonZero()
//      while (nonZeros.hasNext())
//      {
//	    var vNZ : Vector.Element =  nonZeros.next()
//	    var vNZindex : Int = vNZ.index()
//	    var vNZvalue : Double= vNZ.get()
//	    //TODO : why is nonZeros.next not actually returning non-zeros?
//	    if (vNZvalue>0)
//	    {
//	      // TODO: ir is supposed to be one's based?
//	    	mat.ir(nnzCtr)=r+Mat.ioneBased
//	    	cols(nnzCtr)=vNZindex
//	    	vals(nnzCtr)=convertType(vNZvalue)
//	    	nnzCtr+=1
//	    }
//      }
//      r+=1
//    }
//    val isort = Mat.ilexsort2(cols,mat.ir)
//    var i = 0; while (i < cols.length) {mat.data(i) = vals(isort(i)); i+=1}
//    SparseMat.compressInds(cols,ncols,mat.jc,nnz) // sets sdMat's jc
//	return mat
//  }

}