/** block nested join algorithm **/

package qp.operators;

import qp.utils.*;
import java.io.*;
import java.util.*;
import java.lang.*;

public class BlockNested extends Join{

    int batchsize;        // Number of tuples per out batch
    int leftindex;        // Index of the join attribute in left table
    int rightindex;       // Index of the join attribute in right table
    String lfname;        // The file name where the left table is materialize
    String rfname;        // The file name where the right table is materialize

    static int leftfilenum  = 0; // To get unique filenum for this operation
    static int rightfilenum = 0;

    Batch outbatch;       // Output buffer
    Batch leftbatch;      // Buffer for left input stream
    Batch rightbatch;     // Buffer for right input stream
    Batch leftpage;
    Batch rightpage;
    ObjectInputStream r; // File pointer to the right hand materialized file
    ObjectInputStream s; // File pointer to the right hand materialized file

    int lcurs;            // Cursor for left side buffer
    int rcurs;            // Cursor for right side buffer
    boolean eosl;         // Whether end of stream (left table) is reached
    
    // for debugging
    Tuple last = null;
    int distinctRight = 1;
    int distinctLeft = 1;
    
    public BlockNested(Join jn){
		super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
		schema   = jn.getSchema();
		jointype = jn.getJoinType();
		numBuff  = jn.getNumBuff();
    }

    /** During open finds the index of the join attributes
     **  Materializes both left & right hand side into files
     **  Opens the connections
     **/

    public boolean open(){
		/** select number of tuples per batch **/
		int tuplesize       = schema.getTupleSize();
		int pageSize        = Batch.getPageSize();
		int leftTracker     = 0;  // 1 based so that it is easier to debug and read

		batchsize           = pageSize/tuplesize;
		Attribute leftattr  = con.getLhs();
		Attribute rightattr = (Attribute) con.getRhs();
		leftindex           = left.getSchema().indexOf(leftattr);
		rightindex          = right.getSchema().indexOf(rightattr);

		leftpage = new Batch(batchsize * (numBuff-2));
		rightpage = new Batch(batchsize);
 
		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~");
		System.out.printf("numBuff : %d\n", numBuff);
		System.out.printf("pagesize : %d\n", pageSize);
		System.out.printf("tuplesize : %d\n", tuplesize);
		System.out.printf("batchsize : %d\n", batchsize);
		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~");

		/** initialize the cursors of input buffers **/
		lcurs = 0; 
		rcurs = 0;
		eosl  = false;

		if (!left.open()) {
			// error opening left
		    return false;
		} else {
		    leftfilenum++;
		    lfname = "BJtemp-Left-" + String.valueOf(leftfilenum);
		    try{
		    	// Loading B-2 pages of R into memory
				ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(lfname));
				for (int i = 0; i < batchsize * (numBuff-2)+1; i ++){
					Tuple next = left.iteratorNext();
					if (next != null){
						leftpage.add(next);
					} else {
						break;
					}
				}
				out.writeObject(leftpage);
				out.close();
		    } catch (IOException io){
				System.out.println("BlockNested:writing the temporary file error");
				return false;
		    }
		}

		if (!right.open()) {
		    return false;
		} else {
			rightfilenum++;
		    rfname = "BJtemp-Right-" + String.valueOf(rightfilenum);
		    try{
		    	// loading 1 page of S into memory
				ObjectOutputStream out1 = new ObjectOutputStream(new FileOutputStream(rfname));
				for (int i = 0; i < batchsize; i ++){
					Tuple next = right.iteratorNext();
					if (next != null){
						rightpage.add(next);
					} else {
						break;
					}
				}
				out1.writeObject(rightpage);
				out1.close();
		    } catch (IOException io){
				System.out.println("BlockNested:writing the temporary file error");
				return false;
		    }
		}

		System.out.printf("left: %d, right: %d \n",leftpage.size(),rightpage.size());
		return true;
    }

    /** from input buffers selects the tuples satisfying join condition
     ** And returns a page of output tuples
     **/

    // TODO : change to iterator model

    public Batch getBatch(String fname, int batchsize){
    	Batch parsed_batch = new Batch(batchsize);
		try {
		    ObjectInputStream obj = new ObjectInputStream(new FileInputStream(fname));
		    parsed_batch = (Batch)obj.readObject();
		} catch(IOException io){
		    System.err.println("BlockNested:error in reading the batch file " + fname);
		    System.exit(1);
		} catch (ClassNotFoundException c){
		    System.out.println("BlockNested:Some error in deserialization file " + fname);
		    System.exit(1);
		} 
		return parsed_batch;
    }

    public Tuple iteratorNext(){

    	leftbatch = getBatch(lfname,batchsize*(numBuff-2));
    	rightbatch = getBatch(rfname,batchsize);

    	int count = 0;

		while (true){
			
			if (eosl == true){
	    		break;
	    	}
			// if S finish iterating current page
			if (rcurs > (rightbatch.size()-1)){
			    try{
			    	count = 0;
			    	rightpage = new Batch(batchsize);
			    	// get next page of S

					for (int i = 0; i < batchsize; i ++){
						Tuple next = right.iteratorNext();
						if (next != null){
							count++;
							rightpage.add(next);
						} else {
							right.close();
							right.open();
							lcurs++;
							break;
						}
					}
					// System.out.printf("rightBatch: %d\n", count);
					// write new right page
					ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
				    out.writeObject(rightpage);
					out.close();					
					rightbatch = getBatch(rfname,batchsize);
					// reset right cursor
					rcurs = 0;
			    } catch (IOException io){
					System.out.println("BlockNested:writing the temporary file error");
					System.exit(1);
			    }
			} 

			// loading next R tuple
			if (lcurs > (leftbatch.size()-1)){
				// case where finish the current B-2 batch of R
			    try{
					leftpage = new Batch(batchsize * (numBuff-2));
					for (int i = 0; i < batchsize * (numBuff-2) + 1; i ++){
						Tuple next = left.iteratorNext();
						if (next != null){
							leftpage.add(next);
						} else {
							if (i == 0){
								eosl = true;
							}
							break;
						}
					}
					if (eosl == true){
						System.out.println("finished block nested join");
						return null;
					} else {						
						ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(lfname));
						out.writeObject(leftpage);
						out.close();
						lcurs = 0;
						leftbatch = getBatch(lfname, batchsize*(numBuff-2));
					}
			    } catch (IOException io){
					System.out.println("BlockNested:writing the temporary file error");
					System.exit(1);
			    }
			} 

			if (leftbatch.size() == 0){
				// System.out.printf("~~distinctLeft : %d\n", distinctLeft);
				return null;
			}

			if (rightpage.size() == 0){
				continue;
			}

			Tuple nextLeft = leftbatch.elementAt(lcurs);
			Tuple nextRight = rightbatch.elementAt(rcurs++);
			if (last == null){
				last = nextLeft;
			}
			// found matching tuple, return tuple
			if ( Tuple.compareTuples(last,nextLeft,0) == 0){
				distinctRight++;
			} else {
				// System.out.printf("~count :%d  id:%d\n",distinctRight, last.dataAt(0));
				last = nextLeft;
				distinctRight = 1;
				distinctLeft++;
			}

			// Debug.PPrint(nextLeft);
			// Debug.PPrint(nextRight);
			

			if (nextLeft.checkJoin(nextRight,leftindex,rightindex)){
				return nextLeft.joinWith(nextRight);
			}
		}
		return null;

	}

    public void setBatchSize(int batchsize){
    	this.batchsize = batchsize;
    }

    public Batch next(){
		//System.out.print("BlockNested:--------------------------in next----------------");
		// Debug.PPrint(con);
		//System.out.println();
		int i,j;
		
		Tuple nextTuple = iteratorNext();

		if (nextTuple == null){
			return null;
		}

		outbatch = new Batch(batchsize);

		while(!outbatch.isFull() && nextTuple != null){
			outbatch.add(nextTuple);
			nextTuple = iteratorNext();
		}

		return outbatch;
    }

    /** Close the operator */
    public boolean close(){
		File f = new File(lfname);
		f.delete();
		f = new File(rfname);
		f.delete();
		return true;
    }

}