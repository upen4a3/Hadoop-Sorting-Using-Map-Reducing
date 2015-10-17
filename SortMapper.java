
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Secondary sort mapper.
 *
 */
public class SortMapper extends Mapper<LongWritable, Text, NumberKey, IntWritable> {

	private static final Log _log = LogFactory.getLog(SortMapper.class);
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
				
		NumberKey stockKey = new NumberKey(key.get(), Long.valueOf(value.toString()));
		IntWritable stockValue = new IntWritable(Integer.valueOf(value.toString()));
		
		context.write(stockKey, stockValue);
		_log.debug(stockKey.toString() + " => " + stockValue.toString());
	}
}
