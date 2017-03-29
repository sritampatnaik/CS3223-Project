/** Select Operation **/

package qp.operators;

import qp.utils.*;
import java.util.Vector;

public class Select extends Operator{

    Operator base;  // base operator
    Condition con; //select condition
	int batchsize;  // number of tuples per outbatch

    /** The following fields are required during
     ** execution of the select operator
     **/

    Batch inbatch;   // This is the current input buffer
    Batch outbatch;  // This is the current output buffer
    int start, i;       // Cursor position in the input buffer


	/** constructor **/

    public Select(Operator base, Condition con, int type){
	super(type);
	this.base=base;
	this.con=con;

    }

    public void setBase(Operator base){
	this.base = base;
    }

    public Operator getBase(){
	return base;
    }

    public void setCondition(Condition cn){
	this.con=cn;
    }

    public Condition getCondition(){
	return con;
    }


    /** Opens the connection to the base operator
     **/

    public boolean open(){
		/** set number of tuples per page**/
		int tuplesize = schema.getTupleSize();
		batchsize=Batch.getPageSize()/tuplesize;

		if (base.open()) {
			i = 0;
			inbatch= base.next();
		    return true;
		} else {
		    return false;
		}
    }


    /** returns a batch of tuples that satisfies the
     ** condition specified on the tuples coming from base operator
     ** NOTE: This operation is performed on the fly
     **/
    public Tuple iteratorNext(){
		while (true){
			if (inbatch != null && i == inbatch.size()){
	    		inbatch = base.next();
	    		i = 0;
	    	}
	    	// if batch is null means done, return null
	    	if(inbatch == null || inbatch.size() == 0){
			    return null;
			}
			// select tuple
			if (i < inbatch.size()){
				Tuple present = inbatch.elementAt(i++);
				if (checkCondition(present)) {
					return present;
				}
			}
		}		
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


    /** closes the output connection
     ** i.e., no more pages to output
     **/

    public boolean close(){
	/**
	if(base.close())
	    return true;
	else
	  return false;
	  **/
	  return true;
    }



	/** To check whether the selection condition is satisfied for
		the present tuple
		**/

    protected boolean checkCondition(Tuple tuple){
	Attribute attr = con.getLhs();
	int index = schema.indexOf(attr);
	int datatype = schema.typeOf(attr);
	Object srcValue = tuple.dataAt(index);
	String checkValue =(String) con.getRhs();
	int exprtype = con.getExprType();

	if(datatype == Attribute.INT){
	    int srcVal = ((Integer)srcValue).intValue();
	    int checkVal = Integer.parseInt(checkValue);
	    if(exprtype==Condition.LESSTHAN){
		if(srcVal < checkVal)
		    return true;
	    }else if(exprtype==Condition.GREATERTHAN){
		if(srcVal>checkVal)
		    return true;
	    }else if(exprtype==Condition.LTOE){
		if(srcVal<= checkVal)
		    return true;
	    }else if(exprtype==Condition.GTOE){
		if(srcVal>=checkVal)
		    return true;
	    }else if(exprtype==Condition.EQUAL){
		if(srcVal==checkVal)
		    return true;
	    }else if(exprtype==Condition.NOTEQUAL){
		if(srcVal != checkVal)
		    return true;
	    }else{
		System.out.println("Select:Incorrect condition operator");
	    }
	}else if(datatype==Attribute.STRING){
	    String srcVal = (String)srcValue;
	    int flag = srcVal.compareTo(checkValue);

	    if(exprtype==Condition.LESSTHAN){
		if(flag<0) return true;

	    }else if(exprtype==Condition.GREATERTHAN){
		if(flag>0) return true;
	    }else if(exprtype==Condition.LTOE){
		if(flag<=0) return true;
	    }else if(exprtype==Condition.GTOE){
		if(flag>=0) return true;
	    }else if(exprtype==Condition.EQUAL){
		if(flag ==0) return true;
	    }else if(exprtype==Condition.NOTEQUAL){
		if(flag !=0) return true;
	    }else{
		System.out.println("Select: Incorrect condition operator");
	    }

	}else if(datatype==Attribute.REAL){
	    float srcVal = ((Float) srcValue).floatValue();
	    float checkVal = Float.parseFloat(checkValue);
	    if(exprtype==Condition.LESSTHAN){
		if(srcVal<checkVal) return true;
	    }else if(exprtype==Condition.GREATERTHAN){
		if(srcVal>checkVal) return true;
	    }else if(exprtype==Condition.LTOE){
		if(srcVal<=checkVal) return true;
	    }else if(exprtype==Condition.GTOE){
		if(srcVal>=checkVal) return true;
	    }else if(exprtype==Condition.EQUAL){
		if(srcVal==checkVal) return true;
	    }else if(exprtype==Condition.NOTEQUAL){
		if(srcVal!=checkVal) return true;
	    }else{
		System.out.println("Select:Incorrect condition operator");
	    }
	}
	return false;
    }






    public Object clone(){
	Operator newbase = (Operator) base.clone();
	Condition newcon = (Condition) con.clone();
	Select newsel = new Select(newbase,newcon,optype);
	newsel.setSchema(newbase.getSchema());
	return newsel;
    }
}



