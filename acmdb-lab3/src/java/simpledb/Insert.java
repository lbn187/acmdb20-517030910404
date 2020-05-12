package simpledb;
import java.io.*;
/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
	private TransactionId tid;
	private DbIterator child;
	private int tableid;
	private boolean flag;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        // some code goes here
		this.tid = t;
		this.child = child;
		this.tableid = tableId;
		this.flag = true;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
		child.open();
		super.open();
		flag = false;
    }

    public void close() {
        // some code goes here
		super.close();
		child.close();
		flag = true;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
		child.rewind();
		flag = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(flag)return null;
		flag = true;
		int num = 0;
		while(child.hasNext()){
			try{
				Database.getBufferPool().insertTuple(tid, tableid, child.next());
			} catch (DbException | IOException | TransactionAbortedException e){
			}
			num++;
		}
		Tuple tuple = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
		tuple.setField(0, new IntField(num));
		return tuple;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
		child = children[0];
    }
}
