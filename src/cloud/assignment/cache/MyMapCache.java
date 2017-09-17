package cloud.assignment.cache;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MyMapCache extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text word = new Text();
	private Set<String> cacheWords = new HashSet<String>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		try {
			 Path[] localPaths = context.getLocalCacheFiles();
			//URI[] localPaths = context.getCacheFiles();
			if (localPaths != null && localPaths.length > 0) {
				for (Path cacheWordFile : localPaths) {
					BufferedReader buffRead = null;					
					try {
						buffRead = new BufferedReader(new FileReader(cacheWordFile.toString()));
						String cacheWrd = null;
						while ((cacheWrd = buffRead.readLine()) != null) {
							String[] st = cacheWrd.split(" ");
							for (String s : st) {
								cacheWords.add(s.toLowerCase());
							}

						}
					} catch (IOException ex) {
					     ex.getMessage();
					}finally {
						if (buffRead != null)
							try {
									buffRead.close();
								} catch (IOException e) {
									e.printStackTrace();
								}			
					}
				}
			}
		} catch (IOException ex) {
			ex.getMessage();
		}
	}

	

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line, " ");

		while (st.hasMoreTokens()) {
			String wordText = st.nextToken();
			if (cacheWords.contains(wordText.toLowerCase())) {
				System.out.println(wordText);
				word.set(wordText);
				context.write(word,  new IntWritable(1));
			}
		}

	}

}