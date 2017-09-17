package cloud.assignment.cache;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReduceCache extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int num = 0;
		for (IntWritable val : values) {
			num = num + val.get();
		}
		context.write(key, new IntWritable(num));
	}

}
