package ch03;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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

public class Pairs extends Configured implements Tool {

	public static class PairsMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, TextPair, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<TextPair, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] words = line.split("\\s+");

			for (String first : words) {
				for (String second : words) {
					if (!first.equals(second)) {
						output.collect(new TextPair(first, second), one);
					}
				}
			}
		}
	}

	public static class PairsReducer extends MapReduceBase implements
			Reducer<TextPair, IntWritable, TextPair, IntWritable> {

		@Override
		public void reduce(TextPair key, Iterator<IntWritable> values,
				OutputCollector<TextPair, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Pairs(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), getClass());
		conf.setJobName("Pairs");
		conf.setMapperClass(PairsMapper.class);
		conf.setReducerClass(PairsReducer.class);
		conf.setMapOutputKeyClass(TextPair.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setOutputKeyClass(TextPair.class);
		conf.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
		return 0;
	}

}
