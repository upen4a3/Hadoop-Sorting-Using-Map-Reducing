
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class NumberKey implements WritableComparable<NumberKey> {

	private Long offset;
	private Long number;
	
	/**
	 * Constructor.
	 */
	public NumberKey() { }
	
	
	public NumberKey(Long offset, Long number) {
		this.offset = offset;
		this.number = number;
	}
	
	@Override
	public String toString() {
		return (new StringBuilder())
				.append('{')
				.append(offset)
				.append(',')
				.append(number)
				.append('}')
				.toString();
	}
	
	public void readFields(DataInput in) throws IOException {
		offset = in.readLong();
		number = in.readLong();
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(offset);
		out.writeLong(number);
	}

	public int compareTo(NumberKey o) {
		int result = offset.compareTo(o.offset);
		if(0 == result) {
			result = number.compareTo(o.number);
		}
		return result;
	}

	public Long getOffset() {
		return offset;
	}

	public void setOffset(Long offset) {
		this.offset = offset;
	}

	public Long getNumber() {
		return number;
	}

	public void setNumber(Long number) {
		this.number = number;
	}

	

}
