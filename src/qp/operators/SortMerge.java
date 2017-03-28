/** block nested join algorithm **/

package qp.operators;

import qp.utils.*;
import java.io.*;
import java.util.*;
import java.lang.*;

public class SortMerge extends Join{

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
    boolean eosr;         // Whether end of stream (right table) is reached

    Tuple last = null;

    public SortMerge(Join jn){
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
        eosr  = false;

        if (!left.open()) {
            // error opening left
            return false;
        } else {
            leftfilenum++;
            lfname = "BJtemp-Left-" + String.valueOf(leftfilenum);
            try{
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(lfname));
                ArrayList<Tuple> Rlist = new ArrayList<Tuple>();
                Tuple next = left.iteratorNext();

                while (next != null) {
                    Rlist.add(next);
                    next = left.iteratorNext();
                }

                Collections.sort(Rlist, new Comparator<Tuple>() {
                    @Override public int compare(Tuple p1, Tuple p2) {
                        return (Integer)p1.dataAt(leftindex) - (Integer)p2.dataAt(leftindex);
                    }

                });

                Iterator itr=Rlist.iterator();

                //traverse elements of ArrayList object
                while(itr.hasNext()){
                    Tuple st=(Tuple)itr.next();
                    leftpage.add(st);
                }
                out.writeObject(leftpage);
                out.close();
            } catch (IOException io){
                System.out.println("SortMerge:writing the temporary file error");
                return false;
            }
        }

        if (!right.open()) {
            return false;
        } else {
            rightfilenum++;
            rfname = "BJtemp-Right-" + String.valueOf(rightfilenum);
            try{
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                ArrayList<Tuple> Slist = new ArrayList<Tuple>();
                Tuple next = right.iteratorNext();

                while (next != null) {
                    Slist.add(next);
                    next = right.iteratorNext();
                }

                Collections.sort(Slist, new Comparator<Tuple>() {
                    @Override public int compare(Tuple p1, Tuple p2) {
                        return (Integer)p1.dataAt(rightindex) - (Integer)p2.dataAt(rightindex);
                    }

                });

                Iterator itr=Slist.iterator();

                //traverse elements of ArrayList object
                while(itr.hasNext()){
                    Tuple st=(Tuple)itr.next();
                    rightpage.add(st);
                }

                out.writeObject(rightpage);
                out.close();

            } catch (IOException io){
                System.out.println("SortMerge:writing the temporary file error");
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
            System.err.println("SortMerge:error in reading the batch file " + fname);
            System.exit(1);
        } catch (ClassNotFoundException c){
            System.out.println("SortMerge:Some error in deserialization file " + fname);
            System.exit(1);
        }
        return parsed_batch;
    }


    // The actual Iterator model for Join to get next tuple from join
    // but to not break the programme, iteratorNext() is wrapped by
    // next() which returns a page of tuples (which is not compliant to iterator model)
    public Tuple iteratorNext(){
        leftbatch = getBatch(lfname,batchsize);
        rightbatch = getBatch(rfname,batchsize);

        int leftbatchsize = leftbatch.size();
        int rightbatchsize = rightbatch.size();

        Tuple nextLeft = leftbatch.elementAt(lcurs);
        Tuple nextRight = rightbatch.elementAt(rcurs);

        Tuple output = null;
        boolean toSendOutput = false;

        while (!eosl || !eosr ){

            if (lcurs < leftbatchsize) {
                nextLeft = leftbatch.elementAt(lcurs);
            }

            if (rcurs < rightbatchsize) {
                nextRight = rightbatch.elementAt(rcurs);
            }

            int leftIndex = (Integer) nextLeft.dataAt(leftindex);
            int rightIndex = (Integer) nextRight.dataAt(rightindex);

//            Debug.PPrint(nextLeft);
//            Debug.PPrint(nextRight);

            if (leftIndex == rightIndex) {
                output = nextLeft.joinWith(nextRight);
                toSendOutput = true;
            }

            if (leftIndex < rightIndex) {
                if (lcurs < leftbatchsize) {
                    lcurs++;
                }
                System.out.println("lcurs" + lcurs);
            } else {
                if (rcurs < rightbatchsize) {
                    rcurs++;
                }
                System.out.println("rcurs" + rcurs);
            }

            if (lcurs == leftbatchsize - 1){
                eosl = true;
            }
            if (rcurs == rightbatchsize - 1){
                eosr = true;
            }

            if(toSendOutput) {
                toSendOutput = false;
                return output;
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
                Debug.PPrint(nextTuple);
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