package cloud.assignment.cache;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyWordCountCache extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MyWordCountCache(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		//Job job = new Job();
		Job job = Job.getInstance();
		job.setJarByClass(MyWordCountCache.class);
		job.setJobName("Word Count with cache");

		// Add input and output file paths to job based on the arguments passed
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Set the MapClass and ReduceClass in the job
		job.setMapperClass(MyMapCache.class);
		job.setReducerClass(MyReduceCache.class);
		job.addCacheFile(new Path(args[2]).toUri());
		int returnValue = job.waitForCompletion(true) ? 0 : 1;

		if (job.isSuccessful()) {
			System.out.println("Hurraayyyy");
		} else if (!job.isSuccessful()) {
			System.out.println("Try again!!");
		}

		return returnValue;
	}
}