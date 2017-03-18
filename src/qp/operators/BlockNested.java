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
    boolean eosr;         // End of stream (right table)
    
    public BlockNested(Join jn){
		super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
		schema   = jn.getSchema();
		jointype = jn.getJoinType();
		numBuff  = jn.getNumBuff();
    }

    /** During open finds the index of the join attributes
     **  Materializes the right hand side into a file
     **  Opens the connections
     **/

    public boolean open(){
		/** select number of tuples per batch **/
		int tuplesize       = schema.getTupleSize();
		int pageSize        = Batch.getPageSize();
		int leftTracker     = 1;  // 1 based so that it is easier to debug and read

		batchsize           = pageSize/tuplesize;
		Attribute leftattr  = con.getLhs();
		Attribute rightattr = (Attribute) con.getRhs();
		leftindex           = left.getSchema().indexOf(leftattr);
		rightindex          = right.getSchema().indexOf(rightattr);
 
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
		eosr  = false;

		// loading B-2 pages of R into memory
		if (!left.open()) {
			// error opening left
		    return false;
		} else {
		    leftfilenum++;
		    lfname = "BJtemp-Left-" + String.valueOf(leftfilenum);
		    try{
				ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(lfname));
				leftpage = left.next();
				while (leftpage != null && leftTracker < (numBuff - 2)){
				    out.writeObject(leftpage);
				    leftTracker++;
				    leftpage = left.next();
				}
				out.close();
		    } catch (IOException io){
				System.out.println("BlockNested:writing the temporary file error");
				return false;
		    }
		}

		// loading 1 page of S into memory
		if (!right.open()) {
			// error opening right
		    return false;
		} else {
			rightfilenum++;
		    rfname = "BJtemp-Right-" + String.valueOf(rightfilenum);
		    try{
				ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
				rightpage = right.next();
				if (rightpage == null){
					return false;
				}
				out.writeObject(rightpage);
				out.close();
		    } catch (IOException io){
				System.out.println("BlockNested:writing the temporary file error");
				return false;
		    }
		}
		
		return true;
    }

    /** from input buffers selects the tuples satisfying join condition
     ** And returns a page of output tuples
     **/

    // TODO : change to iterator model

    public Tuple interatorNext(){

    	// loading 1 block fo S
		try {
		    s = new ObjectInputStream(new FileInputStream(rfname));
		    rightbatch = (Batch)s.readObject();
		} catch(IOException io){
		    System.err.println("BlockNested:error in reading the s file");
		    System.exit(1);
		} catch (ClassNotFoundException c){
		    System.out.println("BlockNested:Some error in deserialization s");
		    System.exit(1);
		} 

		// loading B-2 blocks of R
    	try {
		    r = new ObjectInputStream(new FileInputStream(lfname));
		    leftbatch = (Batch)r.readObject();
		} catch(IOException io){
		    System.err.println("BlockNested:error in reading the r file");
		    System.exit(1);
		} catch (ClassNotFoundException c){
		    System.out.println("BlockNested:Some error in deserialization r");
		    System.exit(1);
		} 


		while (true){
			
			if (eosl == true){
	    		break;
	    	}
			// if S finish iterating current page
			if (rcurs > (rightbatch.size()-1)){
			    try{
			    	// get next page of S
					rightpage = right.next();
					while (rightpage != null && rightpage.size() == 0){
						rightpage = right.next();
					}

					// next page of S doest exist ie. finished checking for current r tuple
					if (rightpage == null){
						lcurs += 1; // increament to next r tuple
						// reset right
						right.close();
						right.open();
						rightpage = right.next();
					} else {
					}

					//// write next page of S into "memory"
					// purge existing S page file
					// File f = new File(rfname);
					// f.delete();
					// write new right page
					ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));

				    out.writeObject(rightpage);
					out.close();					

					try {
					    s = new ObjectInputStream(new FileInputStream(rfname));
					    rightbatch = (Batch)s.readObject();
					} catch(IOException io){
					    System.err.println("BlockNested:error in reading the s file");
					    System.exit(1);
					} catch (ClassNotFoundException c){
					    System.out.println("BlockNested:Some error in deserialization s");
					    System.exit(1);
					} 
					// reset right cursor
					rcurs = 0;
			    } catch (IOException io){
					System.out.println("BlockNested:writing the temporary file error");
					System.exit(1);
			    }
			} 

			// loading next R tuple
			if (lcurs > (leftbatch.size()-1)){
				int leftTracker     = 1;
				// case where finish the current B-2 batch of R
			    try{
					leftpage = left.next();
					if (leftpage == null){
						// case of no more next page of R ie. finished joining
						System.out.println("finished block nested join");
						eosl = true;
						return null;
					} else {
						//// case of having more pages of R to go
						// to be totally safe, purge old file
						
						ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(lfname));
						// keep writing new pages of R into the B-2 batch
						while (leftpage != null && leftTracker < (numBuff - 2)){
						    out.writeObject(leftpage);
						    leftTracker++;
						    leftpage = left.next();
						}
						out.close();

						try {
						    r = new ObjectInputStream(new FileInputStream(lfname));
						    leftbatch = (Batch)r.readObject();
						} catch(IOException io){
						    System.err.println("BlockNested:error in reading the r file");
						    System.exit(1);
						} catch (ClassNotFoundException c){
						    System.out.println("BlockNested:Some error in deserialization r");
						    System.exit(1);
						} 
					}
			    } catch (IOException io){
					System.out.println("BlockNested:writing the temporary file error");
					System.exit(1);
			    }
			} 

			Tuple nextLeft = leftbatch.elementAt(lcurs);
			Tuple nextRight = rightbatch.elementAt(rcurs++);
			
			// found matching tuple, return tuple
			if (nextLeft.checkJoin(nextRight,leftindex,rightindex)){
				System.out.println("MATCH!");
				return nextLeft.joinWith(nextRight);
			}

		}
		System.out.println("returning null");
		return null;

	}


		// // loading 1 block fo S
		// try {
		//     s = new ObjectInputStream(new FileInputStream(rfname));
		//     rightbatch = (Batch)s.readObject();
		// } catch(IOException io){
		//     System.err.println("BlockNested:error in reading the s file");
		//     System.exit(1);
		// } catch (ClassNotFoundException c){
		//     System.out.println("BlockNested:Some error in deserialization s");
		//     System.exit(1);
		// } 

		// System.out.printf("rightsize = %d, rcurs = %d\n",rightbatch.size(),rcurs);

		// Tuple nextRight = rightbatch.elementAt(rcurs++);
		
		// // loading next R tuple
		// if (lcurs > (leftbatch.size()-1)){
		// 	// case where finish the current B-2 batch of R
		//     try{
		// 		leftpage = left.next();
		// 		if (leftpage == null){
		// 			// case of no more next page of R ie. finished joining
		// 			eosl = true;
		// 			return null;
		// 		} else {
		// 			//// case of having more pages of R to go
		// 			// to be totally safe, purge old file
		// 			File f = new File(lfname);
		// 			f.delete();

		// 			ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(lfname));
		// 			// keep writing new pages of R into the B-2 batch
		// 			while (leftpage != null && leftTracker < (numBuff - 2)){
		// 			    out.writeObject(leftpage);
		// 			    leftTracker++;
		// 			    leftpage = left.next();
		// 			}
		// 			out.close();
		// 		}
		//     } catch (IOException io){
		// 		System.out.println("BlockNested:writing the temporary file error");
		// 		System.exit(1);
		//     }
		// } 

		// loading B-2 blocks of R
  //   	try {
		//     r = new ObjectInputStream(new FileInputStream(lfname));
		//     leftbatch = (Batch)r.readObject();
		// } catch(IOException io){
		//     System.err.println("BlockNested:error in reading the r file");
		//     System.exit(1);
		// } catch (ClassNotFoundException c){
		//     System.out.println("BlockNested:Some error in deserialization r");
		//     System.exit(1);
		// } 

		// // case of still have next in current B-2 batch of R
		// Tuple nextLeft = leftbatch.elementAt(lcurs);
		
		// // System.out.printf("~~~~~~~~\n");
		// // Debug.PPrint(nextRight);
		// // Debug.PPrint(nextLeft);
		// // System.out.println(nextLeft.checkJoin(nextRight,leftindex,rightindex));
		// // Debug.PPrint(nextLeft.joinWith(nextRight));
		// // System.out.printf("~~~~~~~~\n");

		// if (nextLeft.checkJoin(nextRight,leftindex,rightindex)){
		// 	return nextLeft.joinWith(nextRight);
		// } else {
		// 	return interatorNext();
		// }
	
    // }

    public Batch next(){
		//System.out.print("BlockNested:--------------------------in next----------------");
		// Debug.PPrint(con);
		//System.out.println();
		int i,j;
		
		Tuple nextTuple = interatorNext();



		if (nextTuple == null){
			return null;
		}

		outbatch = new Batch(batchsize);

		while(!outbatch.isFull() && nextTuple != null){
			outbatch.add(nextTuple);
			nextTuple = interatorNext();
		}

		return outbatch;

		// if 
		// // finishes when there is no more items left in R to join
		// if (eosl) {
		// 	close();
		//     return null;
		// }

		// 	// new right page is to be fetched
		//     if(rcurs == 0 && eosr == true){ 
		// 		rightbatch = (Batch)right.next(); // fetching right
				
		// 		if (rightbatch == null){
		// 		    // finish iterating S with current batch of R, getting next batch of R
		// 		    if (!left.open()){
		// 			    return null;
		// 			} else{
		// 			    /** If the left operator is not a base table then
		// 			     ** Materialize the intermediate result from left
		// 			     ** into a file
		// 			     **/
		// 			    leftTracker = 1;
		// 			    filenum++;
		// 			    lfname = "BJtemp-" + String.valueOf(filenum);
		// 			    try{
		// 					ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(lfname));
		// 					leftpage = left.next();
		// 					while (leftpage != null && leftTracker < (numBuff - 2)){
		// 					    out.writeObject(leftpage);
		// 					    leftTracker++;
		// 					    leftpage = left.next();
		// 					}
		// 					out.close();
		// 			    } catch (IOException io){
		// 					System.out.println("BlockNested:writing the temporary file error");
		// 					return false;
		// 			    }

		// 			}
		// 			right.close();
		// 			right.open();
		// 			rightbatch = (Batch)right.next(); // fetching right
		// 		}
		// 		try {
		// 		    in = new ObjectInputStream(new FileInputStream(lfname));
		// 		    eosr=false;
		// 		} catch(IOException io){
		// 		    System.err.println("BlockNested:error in reading the file");
		// 		    System.exit(1);
		// 		}
		//     }

		//     while (eosr==false) {
		// 		try {
		// 		    if(rcurs == 0 && lcurs == 0){
		// 				rightbatch = (Batch)in.readObject();
		// 		    }
		// 		    for(i = lcurs; i < leftbatch.size() ; i++){
		// 				for(j = rcurs; j < rightbatch.size() ; j++){
		// 				    Tuple lefttuple = leftbatch.elementAt(i);
		// 				    Tuple righttuple = rightbatch.elementAt(j);
		// 				    if (lefttuple.checkJoin(righttuple,leftindex,rightindex)){
		// 						Tuple outtuple = lefttuple.joinWith(righttuple);
		// 						//Debug.PPrint(outtuple);
		// 						//System.out.println();
		// 						outbatch.add(outtuple);
		// 						if (outbatch.isFull()){
		// 						    if(i == (leftbatch.size()-1) && j == (rightbatch.size()-1)){        //case 1
		// 								lcurs = 0;
		// 								rcurs = 0;
		// 						    } else if (i != (leftbatch.size()-1) && j == (rightbatch.size()-1)){//case 2
		// 								lcurs = i+1;
		// 								rcurs = 0;
		// 						    } else if (i == (leftbatch.size()-1) && j != (rightbatch.size()-1)){//case 3
		// 								lcurs = i;
		// 								rcurs = j+1;
		// 						    } else{
		// 								lcurs = i;
		// 								rcurs = j+1;
		// 						    }
		// 						    return outbatch;
		// 						}
		// 				    }
		// 				}
		// 				rcurs =0;
		// 		    }
		// 		    lcurs=0;
		// 		} catch (EOFException e){
		// 		    try {
		// 				in.close();
		// 		    } catch (IOException io){
		// 				System.out.println("BlockNested:Error in temporary file reading");
		// 		    }
		// 		    eosr=true;
		// 		} catch (ClassNotFoundException c){
		// 		    System.out.println("BlockNested:Some error in deserialization ");
		// 		    System.exit(1);
		// 		} catch (IOException io){
		// 		    System.out.println("BlockNested:temporary file reading error");
		// 		    System.exit(1);
		// 		}
		//     }
		// }
		// return outbatch;
    }

    /** Close the operator */
    public boolean close(){
		File f = new File(lfname);
		f.delete();
		File f = new File(rfname);
		f.delete();
		return true;
    }

}