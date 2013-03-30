import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import BIDMat.FMat
import BIDMat.Mat
import BIDMat.MatFunctions._
import BIDMat.Plotting._
import BIDMat.SciFunctions._
import BIDMat.Solvers._
import BIDMat.{SMat,FMat,IMat,BMat}
import BIDMat.SparseMat
import BIDMat.DenseMat

object TestMatIO {

  def main(args: Array[String]): Unit = {
    Mat.noMKL=true;
	testFMat();
	testSMat();
	testIMat();
  }
  
   def testSMat()
  {
//    var inputMat = sprand(10,15,0.5)
    var inputMat : SMat = sparse(rand(10,15,0.5))
    var matIO : MatIO = new MatIO();
    matIO.mat = inputMat;
    testMat(matIO, inputMat);
  }
  
  def testIMat()
  {
    var inputMat :IMat = 1\2\3 on  4\5\6 on 7\8\9
    var matIO : MatIO = new MatIO();
    matIO.mat = inputMat;
    testMat(matIO,inputMat);
  }
  
  def testFMat()
  {
    var inputMat :FMat = rand(10,15);
    var matIO : MatIO = new MatIO();
    matIO.mat = inputMat;
    testMat(matIO,inputMat);
  }
  
  def testMat (matIO : MatIO, inputMat :Mat)
  {
    Mat.noMKL=true;
    var dOut : ByteArrayOutputStream = new ByteArrayOutputStream();
    matIO.write(new DataOutputStream(dOut));
    matIO.mat = null;

    var dIn : ByteArrayInputStream = new ByteArrayInputStream(dOut.toByteArray());
    matIO.readFields(new DataInputStream(dIn));
//    println(matIO.contents.nrows + " " + matIO.contents.ncols);
   
    matIO.mat match
    {
      case dM : DenseMat[_] => {
        var iM = inputMat match
        {
        	case fMat : FMat => fMat;
        	case iMat : IMat => iMat;
        }
        assert(dM.nrows.equals(iM.nrows))
        assert(dM.ncols.equals(iM.ncols))
        var k = 0;
        while (k < iM.data.length)
        {
          assert(dM.data(k) == iM.data(k));
          k = k + 1
        }
      };
      case sM : SparseMat[_] => {
        var iM = inputMat match
        {
        	case sMat : SMat => sMat;
        }
        assert(sM.nrows.equals(iM.nrows))
        assert(sM.ncols.equals(iM.ncols))
        assert(sM.nnz.equals(iM.nnz))
        assert(sM.ir.length.equals(iM.ir.length))
        assert(sM.jc.length.equals(iM.jc.length))
        assert(sM.data.length.equals(iM.data.length))
        var i, j, k = 0;
        while (i < iM.ir.length)
        {
        	assert(sM.ir(i)==iM.ir(i))
        	i = i + 1;
        }
        while (j < iM.jc.length)
    	{
        	assert(sM.jc(j).equals(iM.jc(j)))
        	j = j + 1;
    	}
        while (k < iM.data.length)
        {
        	assert(sM.data(k)==iM.data(k))
        	k = k + 1;
        }
      };
      
      case _ =>{
        println("Default")
      };
    }
    
    // no equals method =/
//    println(inputMat.equals(matIO.contents));
  }

}