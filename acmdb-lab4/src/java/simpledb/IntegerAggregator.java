package simpledb;
import java.util.*;
/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
	private int gbfield;
	private int afield;
	private Op op;
	private TupleDesc tupledesc;
	private Map<Field, Integer> cntmap, valuemap;
	private Map<Field, Tuple> tuplemap;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
		this.gbfield = gbfield;
		this.afield = afield;
		this.op = what;
		if(gbfield == NO_GROUPING)this.tupledesc = new TupleDesc(new Type[]{Type.INT_TYPE});
		else this.tupledesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
		this.cntmap = new HashMap<>();
		this.valuemap = new HashMap<>();
		this.tuplemap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
		Field field = gbfield == NO_GROUPING ? null : tup.getField(gbfield);
		int tuplevalue = ((IntField) tup.getField(afield)).getValue();
		int newvalue = 0;
		if(op == Op.COUNT){
			if(cntmap.get(field) == null)cntmap.put(field, 1);
			else cntmap.put(field, cntmap.get(field) + 1);
			newvalue = cntmap.get(field);
		}
		if(op == Op.AVG){
			if(cntmap.get(field) == null)cntmap.put(field, 1);
			else cntmap.put(field, cntmap.get(field) + 1);
			if(valuemap.get(field) == null)valuemap.put(field, tuplevalue);
			else valuemap.put(field, valuemap.get(field) + tuplevalue);
			newvalue = valuemap.get(field) / cntmap.get(field);
		}
		if(op == Op.SUM){
			if(valuemap.get(field) == null)valuemap.put(field, tuplevalue);
			else valuemap.put(field, valuemap.get(field) + tuplevalue);
			newvalue = valuemap.get(field);
		}
		if(op == Op.MIN){
			if(valuemap.get(field) == null)valuemap.put(field, tuplevalue);
			else valuemap.put(field, Math.min(valuemap.get(field), tuplevalue));
			newvalue = valuemap.get(field);
		}
		if(op == Op.MAX){
			if(valuemap.get(field) == null)valuemap.put(field, tuplevalue);
			else valuemap.put(field, Math.max(valuemap.get(field), tuplevalue));
			newvalue = valuemap.get(field);
		}
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
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        return new TupleIterator(tupledesc, tuplemap.values());
    }

}
