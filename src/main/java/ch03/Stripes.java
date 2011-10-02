package ch03;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Stripes extends Configured implements Tool {

	public static class StripesMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, MapWritable> {

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, MapWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] words = line.split("\\s+");
			MapWritable countMap = new MapWritable();

			for (String first : words) {
				for (String second : words) {
					if (!first.equals(second)) {
						IntWritable countWritable = (IntWritable)countMap.get( new Text( second ) );
						int count = (countWritable == null) ? 1 : countWritable.get() + 1;
						countMap.put(new Text(second), new IntWritable(count));
					}
				}
				output.collect(new Text(first), countMap);
				countMap.clear();
			}
		}
	}

	public static class StripesReducer extends MapReduceBase implements
			Reducer<Text, MapWritable, TextPair, IntWritable> {

		@Override
		public void reduce(Text key, Iterator<MapWritable> values,
				OutputCollector<TextPair, IntWritable> output, Reporter reporter)
				throws IOException {
			
			MapWritable totalMap = new MapWritable();
			while (values.hasNext()) {
				MapWritable countMap = values.next();
				for(Writable word : countMap.keySet()) {
					IntWritable totalWritable = (IntWritable)totalMap.get( word );
					int total = (totalWritable == null) ? 0 : totalWritable.get();
					IntWritable countWritable = (IntWritable)countMap.get(word);
					int count = countWritable.get();
					totalMap.put(word, new IntWritable(total + count));
				}
			}
			
			for(Writable word : totalMap.keySet()) {
				output.collect(new TextPair(key, (Text)word), (IntWritable)totalMap.get(word));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Stripes(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), getClass());
		conf.setJobName("Stripse");
		conf.setMapperClass(StripesMapper.class);
		conf.setReducerClass(StripesReducer.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(MapWritable.class);
		conf.setOutputKeyClass(TextPair.class);
		conf.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
		return 0;
	}

}
