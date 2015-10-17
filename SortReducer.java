
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class SortReducer extends Reducer<NumberKey, IntWritable, NullWritable, Text> {

	private static final Log _log = LogFactory.getLog(SortReducer.class);
	
	@Override
	public void reduce(NumberKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		Text k = new Text(key.toString());
		int count = 0;
		
		Iterator<IntWritable> it = values.iterator();
		while(it.hasNext()) {
			Text v = new Text(it.next().toString());
			context.write(NullWritable.get(), v);
			_log.debug(k.toString() + " => " + v.toString());
			count++;
		}
		
		_log.debug("count = " + count);
	}
}
