package simpledb;
/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	private int buckets;
	private int min;
	private int max;
	private int size;
	private int sum;
	private int num[];
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
		this.buckets = buckets;
		this.min = min;
		this.max = max;
		this.num = new int[buckets];
		this.sum = 0;
		this.size = (max - min) / buckets + 1;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
		int pos = (v - min) / size;
		++num[pos];
		++sum;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
		int pos = (v - min) / size;
		double w = 0.0;
        if(op == Predicate.Op.EQUALS){
			if(v < min || v > max)return 0.0;
			return 1.0 * num[pos] / size / sum;
		}
		if(op == Predicate.Op.NOT_EQUALS){
			if(v < min || v > max)return 1.0;
			return 1.0 - 1.0 * num[pos] / size / sum;
		}
		if(op == Predicate.Op.LESS_THAN){
			if(v <= min)return 0.0;
			if(v > max)return 1.0;
			for(int i = 0;i < pos; i++)
				w += num[i];
			w += 1.0 * num[pos] * (v - min - pos * size) / size;
			return w / sum;
		}
		if(op == Predicate.Op.LESS_THAN_OR_EQ){
			return estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);
		}
		if(op == Predicate.Op.GREATER_THAN){
			if(v < min)return 1.0;
			if(v >= max)return 0.0;
			for(int i = pos + 1; i < buckets; i++)
				w += num[i];
			w += 1.0 * num[pos] * (min + (pos + 1) * size - v - 1) / size;
			return w / sum;
		}
		if(op == Predicate.Op.GREATER_THAN_OR_EQ){
			return estimateSelectivity(Predicate.Op.GREATER_THAN, v - 1);
		}
		return 0.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
