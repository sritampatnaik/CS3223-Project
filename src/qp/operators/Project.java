/** To projec out the required attributes from the result **/

package qp.operators;

import qp.utils.*;
import java.util.Vector;

public class Project extends Operator{

    Operator base;
    Vector attrSet;
	int batchsize;  // number of tuples per outbatch
	int i;


    /** The following fields are requied during execution
     ** of the Project Operator
     **/

    Batch inbatch;
    Batch outbatch;

    /** index of the attributes in the base operator
     ** that are to be projected
     **/

    int[] attrIndex;


    public Project(Operator base, Vector as,int type){
	super(type);
	this.base=base;
	this.attrSet=as;

    }

    public void setBase(Operator base){
	this.base = base;
    }

    public Operator getBase(){
	return base;
    }

    public Vector getProjAttr(){
	return attrSet;
    }

    /** Opens the connection to the base operator
     ** Also figures out what are the columns to be
     ** projected from the base operator
     **/

    public boolean open(){
		/** setnumber of tuples per batch **/
		int tuplesize = schema.getTupleSize();
		batchsize=Batch.getPageSize()/tuplesize;

		/** The followingl loop findouts the index of the columns that
		 ** are required from the base operator
		 **/

		Schema baseSchema = base.getSchema();
		attrIndex = new int[attrSet.size()];
		for(int i=0;i<attrSet.size();i++){
		    Attribute attr = (Attribute) attrSet.elementAt(i);
	  	    int index = baseSchema.indexOf(attr);
		    attrIndex[i]=index;
		}

		if(base.open()){
			// preloads the table and initialize pointer i
			inbatch = base.next();
			i = 0;
		    return true;
		}
		else
		    return false;
    }

    // The actual Iterator model for Project to get next tuple from Project
    // but to not break the programme, iteratorNext() is wrapped by
    // next() which returns a page of tuples (which is not compliant to iterator model)
    public Tuple iteratorNext(){
    	// if finish scanning current batch,
    	// get next batch and reset pointer
    	if (inbatch != null && i == inbatch.size()){
    		inbatch = base.next();
    		i = 0;
    	}
    	// if batch is null means done, return null
    	if(inbatch == null || inbatch.size() == 0){
		    return null;
		}
    	// project tuple	
		Tuple basetuple = inbatch.elementAt(i++);
		Vector present = new Vector();
		for(int j=0;j<attrSet.size();j++){
			Object data = basetuple.dataAt(attrIndex[j]);
			present.add(data);
	    }
	    return new Tuple(present);
    }

	// the "wrong" next, not iterator model
	// however to not break the system, we just
	// use this and call iteratorNext() to get next tuple
	// to fill up page to return;
    public Batch next(){
		outbatch = new Batch(batchsize);
		// if first tuple is null, then no need to iteratre
		// just return null straight
		Tuple nextTuple = iteratorNext();
		if (nextTuple == null){
			return null;
		}
		outbatch.add(nextTuple);
		// fill up tuples for batch and return
		for(int n=1;n<batchsize;n++){ 
		    nextTuple = iteratorNext();
		    // add only if not null and
		    // if encounter null means finished all,
		    // just return what we have
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
    	base.close();
		return true;
    }


    public Object clone(){
	Operator newbase = (Operator) base.clone();
	Vector newattr = new Vector();
	for(int x=0;x<attrSet.size();x++)
	    newattr.add((Attribute) ((Attribute)attrSet.elementAt(x)).clone());
	Project newproj = new Project(newbase,newattr,optype);
	Schema newSchema = newbase.getSchema().subSchema(newattr);
	newproj.setSchema(newSchema);
	return newproj;
    }
}
