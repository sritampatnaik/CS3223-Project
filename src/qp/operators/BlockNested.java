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

		leftpage = new Batch(batchsize * (numBuff-2));
		rightpage = new Batch(batchsize);
 
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
		    	// Loading B-2 pages of R into memory => num tuples = batchsize * (numBuff-2) 
				ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(lfname));
				for (int i = 0; i < batchsize * (numBuff-2)+1; i ++){
					Tuple next = left.iteratorNext();
					if (next != null){
						leftpage.add(next);
					} else {
						// handle the case where R is completely loaded 
						// ie. (R.iteratorNext() returns null), just pass
						// what i have along
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
		    	// loading 1 page of S into memory => num tuples = batchsize
				ObjectOutputStream out1 = new ObjectOutputStream(new FileOutputStream(rfname));
				for (int i = 0; i < batchsize; i ++){
					Tuple next = right.iteratorNext();
					if (next != null){
						rightpage.add(next);
					} else {
						// handle the case where S is completely loaded 
						// ie. (S.iteratorNext() returns null), just pass
						// what i have along
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


    // The actual Iterator model for Join to get next tuple from join
    // but to not break the programme, iteratorNext() is wrapped by
    // next() which returns a page of tuples (which is not compliant to iterator model)
    public Tuple iteratorNext(){
    	leftbatch = getBatch(lfname,batchsize*(numBuff-2));
    	rightbatch = getBatch(rfname,batchsize);
		while (true){
			if (eosl == true){
	    		break;
	    	}
			// if S finish iterating current page
			if (rcurs > (rightbatch.size()-1)){
			    try{
			    	rightpage = new Batch(batchsize);
			    	// get next page of S
					for (int i = 0; i < batchsize; i ++){
						Tuple next = right.iteratorNext();
						if (next != null){
							rightpage.add(next);
						} else {
							right.close();
							right.open();
							lcurs++;
							break;
						}
					}
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
					// loading next B-2 batch of R
					for (int i = 0; i < batchsize * (numBuff-2) + 1; i ++){
						Tuple next = left.iteratorNext();
						if (next != null){
							leftpage.add(next);
						} else {
							// if first item returns null then just break
							// and return null, since no more tuples anyway
							if (i == 0){
								eosl = true;
							}
							break;
						}
					}
					if (eosl == true){
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

			// no more tuples left, just return
			if (leftbatch.size() == 0){
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
			if (nextLeft.checkJoin(nextRight,leftindex,rightindex)){
				return nextLeft.joinWith(nextRight);
			} 
		}
		return null;

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
			// add tuple if not null;
			if (nextTuple != null){
				nextTuple = iteratorNext();
			}
			outbatch.add(nextTuple);
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