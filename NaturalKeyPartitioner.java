
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitions key based on "natural" key of {@link StockKey} (which
 * is the symbol).
 * @author Jee Vang
 *
 */
public class NaturalKeyPartitioner extends Partitioner<NumberKey, IntWritable> {

	@Override
	public int getPartition(NumberKey key, IntWritable val, int numPartitions) {
		int hash = key.getOffset().hashCode();
		int partition = hash % numPartitions;
		return partition;
	}

}
