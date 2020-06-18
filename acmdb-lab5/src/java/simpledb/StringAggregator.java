package simpledb;
import java.util.*;
/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
	private int gbfield;
	private int afield;
	private Op op;
	private TupleDesc tupledesc;
	private Map<Field, Integer> cntmap;
	private Map<Field, Tuple> tuplemap;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
		this.gbfield = gbfield;
		this.afield = afield;
		if(!Objects.equals(what, Op.COUNT))throw new IllegalArgumentException("what!=COUNT");
		if(gbfield == NO_GROUPING)this.tupledesc = new TupleDesc(new Type[]{Type.INT_TYPE});
		else this.tupledesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
		this.cntmap = new HashMap<>();
		this.tuplemap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
		Field field = gbfield == NO_GROUPING ? null : tup.getField(gbfield);
		if(cntmap.get(field) == null)cntmap.put(field, 1);
		else cntmap.put(field, cntmap.get(field) + 1);
		int newvalue = cntmap.get(field);
		Tuple tuple = new Tuple(tupledesc);
		if(field == null)tuple.setField(0, new IntField(newvalue));
		else{
			tuple.setField(0, field);
			tuple.setField(1, new IntField(newvalue));
		}
		tuplemap.put(field, tuple);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        return new TupleIterator(tupledesc, tuplemap.values());
    }

}
