package qp.optimizer;

import qp.utils.*;
import qp.operators.*;
import java.lang.Math;
import java.util.Vector;
import java.util.Random;
import java.util.BitSet;
import java.util.Hashtable;
import java.util.Enumeration;
import java.io.*;


public class GreedyOptimizer {
	SQLQuery sqlquery;     			// Vector of Vectors of Select + From + Where + GroupBy
    int numJoin;         			// Number of joins in this query plan

 	Hashtable tab_op_hash;          //table name to the Operator
 	Operator root; 					// root of the query plan tree

    Vector projectlist;
    Vector fromlist;
    Vector selectionlist;     		//List of select conditons
    Vector joinlist;          		//List of join conditions
    Vector groupbylist;

    /** constructor **/
    public GreedyOptimizer(SQLQuery sqlquery){
		this.sqlquery      = sqlquery;

		this.projectlist   = (Vector) sqlquery.getProjectList();
		this.fromlist      = (Vector) sqlquery.getFromList();
		this.selectionlist = sqlquery.getSelectionList();
		this.joinlist      = sqlquery.getJoinList();
		this.groupbylist   = sqlquery.getGroupByList();
		this.numJoin       = joinlist.size();
    }

    public Operator getOptimizedPlan(){
    	Operator initialPlan = prepareInitialPlan();
    	Operator finalPlan = initialPlan;
    	return finalPlan;
    }

    public Operator prepareInitialPlan(){
		tab_op_hash = new Hashtable();
		createScanOp();
		createSelectOp();
		if (numJoin !=0) {
		    createJoinOp();
		}
		createProjectOp();
		return root;
    }

    /** Create Scan Operator for each of the table
     ** mentioned in from list
     **/
    public void createScanOp(){
		int numtab = fromlist.size();
        Scan tempop = null;
 
		for(int i=0 ; i<numtab ; i++){  // For each table in from list

		    String tabname = (String) fromlist.elementAt(i);
		    Scan op1 = new Scan(tabname,OpType.SCAN);
	            tempop = op1;

		    /** Read the schema of the table from tablename.md file
		     ** md stands for metadata
		     **/

		    String filename = tabname+".md";
		    try {
				ObjectInputStream _if = new ObjectInputStream(new FileInputStream(filename));
				Schema schm = (Schema) _if.readObject();
				op1.setSchema(schm);
				_if.close();
		    } catch (Exception e) {
				System.err.println("RandomInitialPlan:Error reading Schema of the table" + filename);
				System.exit(1);
		    }
		    tab_op_hash.put(tabname,op1);
		}

       // 12 July 2003 (whtok)
       // To handle the case where there is no where clause
       // selectionlist is empty, hence we set the root to be
       // the scan operator. the projectOp would be put on top of
       // this later in CreateProjectOp 
       if ( selectionlist.size() == 0 ) {
          root = tempop;
          return;
       }
    }

     /** Create Selection Operators for each of the
     ** selection condition mentioned in Condition list
     **/
    public void createSelectOp(){
		Select op1 = null;
	     
		for(int j=0;j<selectionlist.size();j++){
		    Condition cn = (Condition) selectionlist.elementAt(j);
		    if(cn.getOpType() == Condition.SELECT){
				String tabname = cn.getLhs().getTabName();
				//System.out.println("RandomInitial:-------------Select-------:"+tabname);

				Operator tempop = (Operator)tab_op_hash.get(tabname);
			    op1 = new Select(tempop,cn,OpType.SELECT);
				/** set the schema same as base relation **/
				op1.setSchema(tempop.getSchema());

				modifyHashtable(tempop,op1);
			//tab_op_hash.put(tabname,op1);
		    }
		}
		/** The last selection is the root of the plan tre
		 ** constructed thus far
		 **/
		if(selectionlist.size() != 0){
		    root = op1;
		}
	}

    /** create join operators **/
    public void createJoinOp(){
		BitSet bitCList = new BitSet(numJoin);
		int jnnum = RandNumb.randInt(0,numJoin-1);
		Join jn=null;
		/** Repeat until all the join conditions are considered **/
		while(bitCList.cardinality() != numJoin){
		    /** If this condition is already considered chose
		     ** another join condition
		     **/
		    while(bitCList.get(jnnum)){
				jnnum = RandNumb.randInt(0,numJoin-1);
		    }
		   
		    Condition cn = (Condition) joinlist.elementAt(jnnum);
		    String lefttab = cn.getLhs().getTabName();
		    String righttab = ((Attribute) cn.getRhs()).getTabName();

		    // System.out.println("---------JOIN:---------left X right"+lefttab+righttab);

		    Operator left = (Operator) tab_op_hash.get(lefttab);
		    Operator right = (Operator) tab_op_hash.get(righttab);
		    jn = new Join(left,right,cn,OpType.JOIN);
		    jn.setNodeIndex(jnnum);
		    Schema newsche = left.getSchema().joinWith(right.getSchema());
		    jn.setSchema(newsche);
		    /** randomly select a join type**/
		    int numJMeth = JoinType.numJoinTypes();
		    int joinMeth = RandNumb.randInt(0,numJMeth-1);
		    jn.setJoinType(joinMeth);

		    modifyHashtable(left,jn);
		    modifyHashtable(right,jn);

		    Debug.PPrint(left);

		    System.out.println(left.getOperatorSize());

		    //tab_op_hash.put(lefttab,jn);
		    //tab_op_hash.put(righttab,jn);

		    bitCList.set(jnnum);
		}
		/** The last join operation is the root for the
		 ** constructed till now
		 **/
		if(numJoin !=0){
		    root = jn;
		}
    }

    public void createProjectOp(){
		Operator base = root;
        if ( projectlist == null ){
            projectlist = new Vector();
        }

		if(!projectlist.isEmpty()){
		    root = new Project(base,projectlist,OpType.PROJECT);
		    Schema newSchema = base.getSchema().subSchema(projectlist);
		    root.setSchema(newSchema);
		}
    }

    private void modifyHashtable(Operator old, Operator newop){
		Enumeration e=tab_op_hash.keys();
		while(e.hasMoreElements()){
		    String key = (String)e.nextElement();
		    Operator temp = (Operator)tab_op_hash.get(key);
		    if(temp==old){
			tab_op_hash.put(key,newop);
		    }
		}
    }

    /** This is given plan (A X B) X C **/
    protected void transformLefttoRight(Join op, Join left){
		System.out.println("------------------Left to Right neighbor--------------");
		Operator right = op.getRight();
		Operator leftleft = left.getLeft();
		Operator leftright = left.getRight();
		Attribute leftAttr = op.getCondition().getLhs();
		Join temp;

		/** CASE 1 :  ( A X a1b1 B) X b4c4  C     =  A X a1b1 (B X b4c4 C)
		 ** a1b1,  b4c4 are the join conditions at that join operator
		 **/

		if(leftright.getSchema().contains(leftAttr)){
		    System.out.println("----------------CASE 1-----------------");

		    temp = new Join(leftright,right,op.getCondition(),OpType.JOIN);
		    temp.setJoinType(op.getJoinType());
		    temp.setNodeIndex(op.getNodeIndex());
		    op.setLeft(leftleft);
		    op.setJoinType(left.getJoinType());
		    op.setNodeIndex(left.getNodeIndex());
		    op.setRight(temp);
		    op.setCondition(left.getCondition());

		}else{
		    System.out.println("--------------------CASE 2---------------");
		    /**CASE 2:   ( A X a1b1 B) X a4c4  C     =  B X b1a1 (A X a4c4 C)
		     ** a1b1,  a4c4 are the join conditions at that join operator
		     **/
		    temp = new Join(leftleft,right,op.getCondition(),OpType.JOIN);
		    temp.setJoinType(op.getJoinType());
		    temp.setNodeIndex(op.getNodeIndex());
		    op.setLeft(leftright);
		    op.setRight(temp);
		    op.setJoinType(left.getJoinType());
		    op.setNodeIndex(left.getNodeIndex());
		    Condition newcond = left.getCondition();
		    newcond.flip();
		    op.setCondition(newcond);
		}
    }

    protected void transformRighttoLeft(Join op, Join right){
		System.out.println("------------------Right to Left Neighbor------------------");
		Operator left = op.getLeft();
		Operator rightleft = right.getLeft();
		Operator rightright = right.getRight();
		Attribute rightAttr = (Attribute) op.getCondition().getRhs();
		Join temp;
		/** CASE 3 :  A X a1b1 (B X b4c4  C)     =  (A X a1b1 B ) X b4c4 C
		 ** a1b1,  b4c4 are the join conditions at that join operator
		 **/
		if(rightleft.getSchema().contains(rightAttr)){
		    System.out.println("----------------------CASE 3-----------------------");
		    temp = new Join(left,rightleft,op.getCondition(),OpType.JOIN);
		    temp.setJoinType(op.getJoinType());
		    temp.setNodeIndex(op.getNodeIndex());
		    op.setLeft(temp);
		    op.setRight(rightright);
		    op.setJoinType(right.getJoinType());
		    op.setNodeIndex(right.getNodeIndex());
		    op.setCondition(right.getCondition());
		}else{
		    /** CASE 4 :  A X a1c1 (B X b4c4  C)     =  (A X a1c1 C ) X c4b4 B
		     ** a1b1,  b4c4 are the join conditions at that join operator
		     **/
		    System.out.println("-----------------------------CASE 4-----------------");
		    temp = new Join(left,rightright,op.getCondition(),OpType.JOIN);
		    temp.setJoinType(op.getJoinType());
		    temp.setNodeIndex(op.getNodeIndex());

		    op.setLeft(temp);
		    op.setRight(rightleft);
		    op.setJoinType(right.getJoinType());
		    op.setNodeIndex(right.getNodeIndex());
		    Condition newcond = right.getCondition();
		    newcond.flip();
		    op.setCondition(newcond);

		}
    }

    /** This method traverses through the query plan and
     *  returns the node mentioned by joinNum
     **/
    protected Operator findNodeAt(Operator node,int joinNum){
		if(node.getOpType() == OpType.JOIN){
		    if(((Join)node).getNodeIndex()==joinNum){
				return node;
		    } else{
				Operator temp;
				temp= findNodeAt(((Join)node).getLeft(),joinNum);
				if(temp==null){
					temp = findNodeAt(((Join)node).getRight(),joinNum);
					return temp;
				}
			}
		}else if(node.getOpType() == OpType.SCAN){
	    	return null;
		}else if(node.getOpType()==OpType.SELECT){
	    	//if sort/project/select operator
	    	return findNodeAt(((Select)node).getBase(),joinNum);
		}else if(node.getOpType()==OpType.PROJECT){
	    	return findNodeAt(((Project)node).getBase(),joinNum);
		}

	    return null;
    }


    /** modifies the schema of operators which are modified due to selecing an alternative neighbor plan **/
    private void modifySchema(Operator node){
		if(node.getOpType()==OpType.JOIN){
		    Operator left = ((Join)node).getLeft();
		    Operator right =((Join)node).getRight();
		    modifySchema(left);
		    modifySchema(right);
		    node.setSchema(left.getSchema().joinWith(right.getSchema()));
		}else if(node.getOpType()==OpType.SELECT){
		    Operator base= ((Select)node).getBase();
		    modifySchema(base);
		    node.setSchema(base.getSchema());
		}else if(node.getOpType()==OpType.PROJECT){
		    Operator base = ((Project)node).getBase();
		    modifySchema(base);
		    Vector attrlist = ((Project)node).getProjAttr();
		    node.setSchema(base.getSchema().subSchema(attrlist));
		}
    }

    /** 
     * After finding a choice of method for each operator
	 * prepare an execution plan by replacing the methods with
	 * corresponding join operator implementation
	 **/
    public static Operator makeExecPlan(Operator node){

		if(node.getOpType()==OpType.JOIN){
		    Operator left = makeExecPlan(((Join)node).getLeft());
		    Operator right = makeExecPlan(((Join)node).getRight());
		    int joinType = ((Join)node).getJoinType();
		    int numbuff = BufferManager.getBuffersPerJoin();
		    switch(joinType){
			    case JoinType.NESTEDJOIN:

					NestedJoin nj = new NestedJoin((Join) node);
					nj.setLeft(left);
					nj.setRight(right);
					nj.setNumBuff(numbuff);
					return nj;

			    case JoinType.BLOCKNESTED:

					BlockNested bj = new BlockNested((Join) node);
					bj.setLeft(left);
					bj.setRight(right);
					bj.setNumBuff(numbuff);
		                /* + other code */
					return bj;

			    case JoinType.SORTMERGE:

					NestedJoin sm = new NestedJoin((Join) node);
		                /* + other code */
					return sm;

			    case JoinType.HASHJOIN:

					NestedJoin hj = new NestedJoin((Join) node);
		                /* + other code */
					return hj;
			    default:
					return node;
		    }
		}else if(node.getOpType() == OpType.SELECT){
		    Operator base = makeExecPlan(((Select)node).getBase());
		    ((Select)node).setBase(base);
		    return node;
		}else if(node.getOpType() == OpType.PROJECT){
		    Operator base = makeExecPlan(((Project)node).getBase());
		    ((Project)node).setBase(base);
		    return node;
		}else{
		    return node;
		}
    }
}