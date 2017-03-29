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
    Tuple next,nextLeft,nextRight;
    Tuple last = null;
    
    public BlockNested(Join jn){
		super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
		schema   = jn.getSchema();
		jointype = jn.getJoinType();
		numBuff  = jn.getNumBuff();
    }

    /** Assumes that materalizing to file means import to memory
     ** to prep, load B-2 pages of R into memory and load 1 page of S into memory
     **/

    public boolean open(){
		/** select number of tuples per batch **/
		int tuplesize       = schema.getTupleSize();
		int pageSize        = Batch.getPageSize();

		batchsize           = pageSize/tuplesize;
		Attribute leftattr  = con.getLhs();
		Attribute rightattr = (Attribute) con.getRhs();
		leftindex           = left.getSchema().indexOf(leftattr);
		rightindex          = right.getSchema().indexOf(rightattr);

		System.out.print("BlockNested Join: ");
		Debug.PPrint(con);
		System.out.println();

		leftpage = new Batch(batchsize * (numBuff-2));
		rightpage = new Batch(batchsize);
 
		/** initialize the cursors of input buffers **/
		lcurs = 0; 
		rcurs = 0;

		if (!left.open()) {
			// error opening left
		    return false;
		} else {
			// loading B-2 pages worth of Left data into buffer
			for (int i = 0; i < batchsize * (numBuff-2); i ++){
				next = left.iteratorNext();
				if (next != null){
					leftpage.add(next);
				} else {
					break;
				}	
			}
		}

		if (!right.open()) {
			// error opening right
		    return false;
		} else {
			// loading 1 page worth of Left data into buffer
			for (int i = 0; i < batchsize; i ++){
				next = right.iteratorNext();
				if (next != null){
					rightpage.add(next);
				} else {
					break;
				}	
			}
		}
		return true;
    }

    // The actual Iterator model for Join to get next tuple from join
    // but to not break the programme, iteratorNext() is wrapped by
    // next() which returns a page of tuples (which is not compliant to iterator model)
    public Tuple iteratorNext(){
    	while (true){
	    	if (rcurs < rightpage.size()){
	    		nextRight = rightpage.elementAt(rcurs++);
	    	} else {
	    		// get next set of 1 page worth of right 
				rightpage = new Batch(batchsize);
				next = right.iteratorNext();
				// check first item of new batch, if no more
				// then loop around
				if (next == null){
					right.close();
					right.open();
					lcurs++;
					next = right.iteratorNext();
				} 
				rightpage.add(next);
				
				// loading B-2 pages worth of right data into buffer
				for (int i = 1; i < batchsize ; i ++){
					next = right.iteratorNext();
					if (next != null){
						rightpage.add(next);
					} else {
						break;
					}	
				}
				//reset pointer
				rcurs = 0;
				nextRight = rightpage.elementAt(rcurs++);
	    	}
	    	if (lcurs < leftpage.size()){
	    		nextLeft = leftpage.elementAt(lcurs);
	    	} else {
	    		// get next set of B-2 worth of left 
				leftpage = new Batch(batchsize * (numBuff-2));
				next = left.iteratorNext();
				// check first item of new batch, if no more
				// new item then just return null as job is completed
				if (next == null){
					return null;
				} else {
					leftpage.add(next);
				}
				// loading B-2 pages worth of Left data into buffer
				for (int i = 1; i < batchsize * (numBuff-2); i ++){
					next = left.iteratorNext();
					if (next != null){
						leftpage.add(next);
					} else {
						break;
					}	
				}
				//reset pointer
				lcurs = 0;
	    	}
	    	if (nextLeft.checkJoin(nextRight,leftindex,rightindex)){
				return nextLeft.joinWith(nextRight);
			} 
		}
	}

	// the "wrong" next, not iterator model
	// however to not break the system, we just
	// use this and call iteratorNext() to get next tuple
	// to fill up page to return;
    public Batch next(){
		Batch outbatch = new Batch(batchsize);
		Tuple nextTuple = iteratorNext();
		// check if there is no tuple at all just return null;
		if (nextTuple == null){
			return null;
		} else {
			outbatch.add(nextTuple);
		}
		for (int i = 1; i < batchsize; i ++){
			nextTuple = iteratorNext();
			// add tuple if not null;
			if (nextTuple != null){
				outbatch.add(nextTuple);
			} else {
				break;
			}
		}
		return outbatch;
    }

    /** Close the operator */
    public boolean close(){
    	left.close();
    	right.close();
		return true;
    }

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


}