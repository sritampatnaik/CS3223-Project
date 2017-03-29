/** performs randomized optimization, simulated annealing algorithm **/


package qp.optimizer;

import qp.utils.*;
import qp.operators.*;
import java.lang.Math;
import java.util.Vector;
import java.util.Random;

public class SARandomOptimizer{

	/** enumeration of different ways to find the neighbor plan **/
    public static final int  METHODCHOICE =0;  //selecting neighbor by changing a method for an operator
    public static final int COMMUTATIVE =1;   // by rearranging the operators by commutative rule
    public static final int ASSOCIATIVE =2;  // rearranging the operators by associative rule
    public static final int DEFAULTT = 10000;
    /** Number of altenative methods available for a node as specified above**/
    public static final int NUMCHOICES = 3;

    SQLQuery sqlquery;     // Vector of Vectors of Select + From + Where + GroupBy
    int numJoin;          // Number of joins in this query plan

	/** constructor **/
    public SARandomOptimizer(SQLQuery sqlquery){
		this.sqlquery = sqlquery;
    }

    /** Randomly selects a neighbour **/
    protected Operator getNeighbor(Operator root){
		//Randomly select a node to be altered to get the neighbour
		int nodeNum = RandNumb.randInt(0,numJoin-1);

		//Randomly select type of alteration: Change Method/Associative/Commutative
		int changeType = RandNumb.randInt(0,NUMCHOICES-1);
		Operator neighbor = null;

		switch(changeType){
		case  METHODCHOICE:   // Select a neighbour by changing the method type
		    neighbor = neighborMeth(root,nodeNum);
		    break;
		case COMMUTATIVE:
		    neighbor=neighborCommut(root,nodeNum);
		    break;
		case ASSOCIATIVE:
		    neighbor=neighborAssoc(root,nodeNum);
		    break;
		}
		return neighbor;
    }


    /** implementation of Iterative Improvement Algorithm
     ** for Randomized optimization of Query Plan
     **/

    public Operator getOptimizedPlan(){

	/** get an initial plan for the given sql query **/

	RandomInitialPlan rip = new RandomInitialPlan(sqlquery);
	 numJoin = rip.getNumJoins();

	int MINCOST = Integer.MAX_VALUE;
	Operator finalPlan = null;


	/** NUMTER is number of times the inner loop in simulated annealing should run **/

	int NUMITER ;
    if(numJoin!=0){
    	NUMITER = 16 * numJoin;
    }else{
		NUMITER=1;
    }

	Operator initPlan = rip.prepareInitialPlan();
	modifySchema(initPlan);
	System.out.println("-----------initial Plan-------------");
	Debug.PPrint(initPlan);
	PlanCost pc = new PlanCost();
	int initCost = pc.getCost(initPlan);
	System.out.println(initCost);

	//Setting the initial temparature (T) for annealing where
	int temparature = 2 * initCost;

	int minNeighborCost=initCost; //just initialization purpose;
	Operator minNeighbor=initPlan; //just initialization purpose;

	int minUnchanged = 0;

	// Loop through while not frozen
	while (temparature >= 1 && minUnchanged < 4){
	    if(numJoin !=0){
			for(int j=0;j<NUMITER;j++) {  // equillibrium
	 			System.out.println("---------------while--------");
			    Operator initPlanCopy = (Operator) initPlan.clone();
			    minNeighbor = getNeighbor(initPlanCopy);

			    System.out.println("--------------------------neighbor---------------");
			    Debug.PPrint(minNeighbor);
			    pc = new PlanCost();
			    minNeighborCost = pc.getCost(minNeighbor);
			    System.out.println("  "+minNeighborCost);

			    for(int i=1; i < 2 * numJoin;i++){
					initPlanCopy= (Operator) initPlan.clone();
					Operator neighbor = getNeighbor(initPlanCopy);
					System.out.println("------------------neighbor--------------");
					Debug.PPrint(neighbor);
					pc=new PlanCost();
					int neighborCost = pc.getCost(neighbor);
					System.out.println(neighborCost);


					if(neighborCost<minNeighborCost){
					    minNeighbor = neighbor;
					    minNeighborCost = neighborCost;
					}
					// System.out.println("-----------------for-------------");
			    }

			    int costDiff = minNeighborCost - initCost;
			    if (costDiff <= 0){
			    	initPlan = minNeighbor;
					initCost = minNeighborCost;
			    } else if (costDiff > 0){
			    	if( Math.random() <= Math.exp( -costDiff / temparature)) {
						initPlan = minNeighbor;
						initCost = minNeighborCost;
					}
			    }

			    if (initCost < minNeighborCost) {
			    	minNeighbor = initPlan;
					minNeighborCost = initCost;
			    }	 
			}


			System.out.println("------------------After Staging (Once equillibrium has reached)--------------");
			Debug.PPrint(minNeighbor);
			System.out.println(" "+minNeighborCost);
	    }
	    temparature *= 0.95;
	    if(minNeighborCost < MINCOST){
			MINCOST = minNeighborCost;
			finalPlan = minNeighbor;
	    }
	}
	System.out.println("\n\n\n");
	System.out.println("---------------------------Plan After FROZEN----------------");
	Debug.PPrint(finalPlan);
	System.out.println("  "+MINCOST);
	return finalPlan;
    }



    /** Selects a random method choice for join wiht number joinNum
     **  e.g., Nested loop join, Sort-Merge Join, Hash Join etc..,
     ** returns the modified plan
     **/

    protected Operator neighborMeth(Operator root, int joinNum){
		System.out.println("------------------neighbor by method change----------------");
		int numJMeth = JoinType.numJoinTypes();
	    if (numJMeth > 1) {
			/** find the node that is to be altered **/
			Join node = (Join) findNodeAt(root,joinNum);
			int prevJoinMeth = node.getJoinType();
			int joinMeth = RandNumb.randInt(0,numJMeth-1);
			while(joinMeth == prevJoinMeth){
			    joinMeth = RandNumb.randInt(0,numJMeth-1);
			}
			node.setJoinType(joinMeth);
	    }
	return root;
    }



    /** Applies join Commutativity for the join numbered with joinNum
     **  e.g.,  A X B  is changed as B X A
     ** returns the modifies plan
     **/

    protected Operator neighborCommut(Operator root, int joinNum){
		System.out.println("------------------neighbor by commutative---------------");
		/** find the node to be altered**/
		Join node = (Join) findNodeAt(root,joinNum);
		Operator left = node.getLeft();
		Operator right = node.getRight();
		node.setLeft(right);
		node.setRight(left);
		/*** also flip the condition i.e.,  A X a1b1 B   = B X b1a1 A  **/
		node.getCondition().flip();
		//Schema newschem = left.getSchema().joinWith(right.getSchema());
		// node.setSchema(newschem);

		/** modify the schema before returning the root **/
		modifySchema(root);
		return root;
    }

    /** Applies join Associativity for the join numbered with joinNum
     **  e.g., (A X B) X C is changed to A X (B X C)
     **  returns the modifies plan
     **/

    protected Operator neighborAssoc(Operator root,int joinNum){
		/** find the node to be altered**/
		Join op =(Join) findNodeAt(root,joinNum);
		//Join op = (Join) joinOpList.elementAt(joinNum);
		Operator left = op.getLeft();
		Operator right = op.getRight();

		if(left.getOpType() == OpType.JOIN && right.getOpType() != OpType.JOIN){
		    transformLefttoRight(op,(Join) left);
		}else if(left.getOpType() != OpType.JOIN && right.getOpType()==OpType.JOIN){
		    transformRighttoLeft(op,(Join) right);
		}else if(left.getOpType()==OpType.JOIN && right.getOpType()==OpType.JOIN){
		    if(RandNumb.flipCoin())
			transformLefttoRight(op,(Join) left);
		    else
			transformRighttoLeft(op,(Join) right);
		}else{
		    // The join is just A X B,  therefore Association rule is not applicable
		}
		/** modify the schema before returning the root **/
		modifySchema(root);
		return root;
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
     ** returns the node mentioned by joinNum
     **/

    protected Operator findNodeAt(Operator node,int joinNum){
		if(node.getOpType() == OpType.JOIN){
		    if(((Join)node).getNodeIndex()==joinNum){
				return node;
		    }else{
				Operator temp;
				temp= findNodeAt(((Join)node).getLeft(),joinNum);
				if(temp==null)
				    temp = findNodeAt(((Join)node).getRight(),joinNum);
					return temp;
			}
		}else if(node.getOpType() == OpType.SCAN){
	    	return null;
		}else if(node.getOpType()==OpType.SELECT){
	    	//if sort/project/select operator
	    	return findNodeAt(((Select)node).getBase(),joinNum);
		}else if(node.getOpType()==OpType.PROJECT){
	    return findNodeAt(((Project)node).getBase(),joinNum);
		}else{
	    	return null;
		}
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



	/** AFter finding a choice of method for each operator
		prepare an execution plan by replacing the methods with
		corresponding join operator implementation
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

		    /** Temporarity used simple nested join,
		    	replace with hasjoin, if implemented **/

		    case JoinType.BLOCKNESTED:

				BlockNested bj = new BlockNested((Join) node);
				bj.setLeft(left);
				bj.setRight(right);
				bj.setNumBuff(numbuff);
	                /* + other code */
			return bj;

		    case JoinType.SORTMERGE:

				SortMerge sm = new SortMerge((Join) node);
				sm.setLeft(left);
				sm.setRight(right);
				sm.setNumBuff(numbuff);
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












