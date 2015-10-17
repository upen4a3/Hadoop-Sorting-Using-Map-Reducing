
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class NaturalKeyGroupingComparator extends WritableComparator {

	/**
	 * Constructor.
	 */
	protected NaturalKeyGroupingComparator() {
		super(NumberKey.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		NumberKey k1 = (NumberKey)w1;
		NumberKey k2 = (NumberKey)w2;
		
		return k1.getNumber().compareTo(k2.getNumber());
	}
}