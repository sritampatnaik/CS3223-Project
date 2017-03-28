/** page nested join algorithm **/

package qp.operators;

import qp.utils.*;
import java.io.*;
import java.util.*;
import java.lang.*;

public class NestedJoin extends Join{


	int batchsize;  //Number of tuples per out batch

    /** The following fields are useful during execution of
     ** the NestedJoin operation
     **/
    int leftindex;     // Index of the join attribute in left table
    int rightindex;    // Index of the join attribute in right table

    String rfname;    // The file name where the right table is materialize

    static int filenum=0;   // To get unique filenum for this operation

    Batch outbatch;   // Output buffer
    Batch leftbatch;  // Buffer for left input stream
    Batch rightbatch;  // Buffer for right input stream
    ObjectInputStream in; // File pointer to the right hand materialized file

    int lcurs;    // Cursor for left side buffer
    int rcurs;    // Cursor for right side buffer
    boolean eosl;  // Whether end of stream (left table) is reached
    boolean eosr;  // End of stream (right table)
    Tuple nextLeft;
	Tuple nextRight; 

    public NestedJoin(Join jn){
	super(jn.getLeft(),jn.getRight(),jn.getCondition(),jn.getOpType());
		schema = jn.getSchema();
		jointype = jn.getJoinType();
		numBuff = jn.getNumBuff();
    }


    public boolean open(){

		/** select number of tuples per batch **/
		int tuplesize=schema.getTupleSize();
		batchsize = Batch.getPageSize()/tuplesize;

		Attribute leftattr = con.getLhs();
		Attribute rightattr =(Attribute) con.getRhs();
		leftindex = left.getSchema().indexOf(leftattr);
		rightindex = right.getSchema().indexOf(rightattr);
		
		Batch rightpage;

		// load first S tuple
		if(!right.open()){
		    return false;
		} else{
		 	nextRight = right.iteratorNext();
		}
		// load first R tuple
		if(left.open()){
			nextLeft = left.iteratorNext();
		    return true;
		}
		else
		    return false;
	}

	// The actual Iterator model for Join to get next tuple from join
    // but to not break the programme, iteratorNext() is wrapped by
    // next() which returns a page of tuples (which is not compliant to iterator model)
	public Tuple iteratorNext(){
		while (true){
			// finish scanning through S
			if (nextRight == null){
				// Restart S
				right.close();
				right.open();
				nextRight = right.iteratorNext();
				// load next R tuple
				nextLeft = left.iteratorNext();
				// just a guard case, should not happen, but if
				// some how right table is empty its getting caught
				// by this guard and return null
				if (nextRight == null){
					return null;
				}
			}
			// finish R means done, just return null
			if (nextLeft == null){
				break;
			}
			if (nextLeft.checkJoin(nextRight,leftindex,rightindex)){
				Tuple temp = nextRight;
				nextRight = right.iteratorNext();
				return nextLeft.joinWith(temp);
			}
			nextRight = right.iteratorNext();
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
		left.close();
		right.close();
		return true;
    }
}











































