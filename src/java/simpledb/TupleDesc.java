package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }
	private Vector<TDItem> TDItemList;
	private Map<String, Integer> NameMap;
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
		return TDItemList.iterator();
        //return null;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
		assert (typeAr.length > 0) && (typeAr.length == fieldAr.length);
		TDItemList = new Vector<>();
		NameMap = new HashMap<>();
		for(int i = 0; i < typeAr.length; i++) {
			TDItemList.add(new TDItem(typeAr[i], fieldAr[i]));
			NameMap.put(fieldAr[i], i);
		}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.。
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
		assert typeAr.length > 0;
		TDItemList = new Vector<>();
		NameMap = new HashMap<>();
		for(int i = 0; i < typeAr.length; i++) {
			TDItemList.add(new TDItem(typeAr[i], null));
		}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return TDItemList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
		if (i < 0 || i >= TDItemList.size()) throw new NoSuchElementException();
        return TDItemList.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= TDItemList.size()) throw new NoSuchElementException();
        return TDItemList.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
		if(name == null)throw new NoSuchElementException();
		if(NameMap.get(name) != null)
			return NameMap.get(name);
		throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int sum = 0;
		for(TDItem item : TDItemList){
			sum += item.fieldType.getLen();
		}
		return sum;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
		int n1 = td1.numFields();
		int n2 = td2.numFields();
		Type[] types = new Type[n1 + n2];
		String[] names = new String[n1 + n2];
		for(int i = 0; i < n1; i++){
			names[i] = td1.getFieldName(i);
			types[i] = td1.getFieldType(i);
		}
		for(int i = 0; i < n2; i++){
			names[i + n1] = td2.getFieldName(i);
			types[i + n1] = td2.getFieldType(i);
		}
		return new TupleDesc(types, names);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
		if(!(o instanceof TupleDesc))return false;
		if(((TupleDesc)o).numFields() != TDItemList.size())return false;
		for(int i = 0; i < TDItemList.size(); i++){
			if(!TDItemList.get(i).fieldType.equals(((TupleDesc)o).getFieldType(i)))
				return false;
		}
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
		String name = "";
		for(int i = 0; i < TDItemList.size(); i++){
			if(i > 0)name +=",";
			name += TDItemList.get(i).fieldType.name();
			name += "(";
			name += TDItemList.get(i).fieldName;
			name += ")";
		}
        return name;
    }
}

