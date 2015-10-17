
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class CompositeKeyComparator extends WritableComparator {

	/**
	 * Constructor.
	 */
	protected CompositeKeyComparator() {
		super(NumberKey.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		NumberKey k1 = (NumberKey)w1;
		NumberKey k2 = (NumberKey)w2;
		
		//descending
		//int	result = -1* k1.getNumber().compareTo(k2.getNumber());
		//ascending
		int	result =  k1.getNumber().compareTo(k2.getNumber());
		return result;
	}
}
