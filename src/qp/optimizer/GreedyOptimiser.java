package qp.optimizer;

import qp.utils.*;
import qp.operators.*;
import java.lang.Math;
import java.util.Vector;
import java.util.Random;

public class GreedyOptimizer {
	SQLQuery sqlquery;     // Vector of Vectors of Select + From + Where + GroupBy
    int numJoin;          // Number of joins in this query plan

    /** constructor **/
    public GreedyOptimizer(SQLQuery sqlquery){
		this.sqlquery = sqlquery;
    }

    public Operator getOptimizedPlan(){
    	Operator finalPlan = null;
    	return finalPlan;
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
