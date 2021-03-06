package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class HashEquiJoin extends Operator {

    private static final long serialVersionUID = 1L;
	private JoinPredicate joinpredicate;
	private DbIterator child1;
	private DbIterator child2;
	private Map<Field, Integer> map;
	private int id;
	private Vector<Vector<Tuple> > vec;
	private Tuple tuple1;
	private int nv;
    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public HashEquiJoin(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
		this.joinpredicate = p;
		this.child1 = child1;
		this.child2 = child2;
		this.map = new HashMap<>();
		this.id = 0;
		this.vec = new Vector<>();
		this.tuple1 = null;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return joinpredicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }
    
    public String getJoinField1Name()
    {
        // some code goes here
		return child1.getTupleDesc().getFieldName(joinpredicate.getField1());
    }

    public String getJoinField2Name()
    {
        // some code goes here
        return child2.getTupleDesc().getFieldName(joinpredicate.getField2());
    }
    
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
		map.clear();
		vec.clear();
		child1.open();
		child2.open();
		while(child2.hasNext()){
			Tuple tuple = child2.next();
			Field field = tuple.getField(joinpredicate.getField2());
			if(map.get(field) == null){
				vec.add(new Vector<>());
				map.put(field, id++);
			}
			vec.get(map.get(field)).add(tuple);
		}
		super.open();
    }

    public void close() {
        // some code goes here
		super.close();
		map.clear();
		vec.clear();
		child1.close();
		child2.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
		child1.rewind();
		child2.rewind();
    }

    transient Iterator<Tuple> listIt = null;

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, there will be two copies of the join attribute in
     * the results. (Removing such duplicate columns can be done with an
     * additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        while(true){
			if(tuple1 == null){
				if(!child1.hasNext())return null;
				tuple1 = child1.next();
				nv = 0;
			}
			Field field1 = tuple1.getField(joinpredicate.getField1());
			if(map.get(field1) == null){
				tuple1 = null;
				continue;
			}
			int num = map.get(field1);
			assert num < vec.size();
			if(nv >= vec.get(num).size()){
				tuple1 = null;
				continue;
			}
			return Tuple.merge(tuple1, vec.get(num).get(nv++));
		}
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{child1, child2};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
		child1 = children[0];
		child2 = children[1];
    }
    
}
