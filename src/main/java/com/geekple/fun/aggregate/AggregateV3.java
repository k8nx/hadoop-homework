package com.geekple.fun.aggregate;

import java.io.IOException;

import com.geekple.fun.common.EmploeeTuple;
import com.geekple.fun.common.Tuple;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Combiner 추가
 * @author Daegeun Kim
 */
@SuppressWarnings("deprecation")
public class AggregateV3 extends Configured implements Tool {
	public static class EmploeeMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, LongWritable, LongWritable>.Context context) throws IOException,
				InterruptedException {
			Tuple tuple = EmploeeTuple.parse(value.toString());
			context.write(tuple.getField(LongWritable.class, EmploeeTuple.DEPTNO),
					tuple.getField(LongWritable.class, EmploeeTuple.SAL));
		}
	}

	public static class SumReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
		protected void reduce(LongWritable key, Iterable<LongWritable> values,
				Reducer<LongWritable, LongWritable, LongWritable, LongWritable>.Context context) throws IOException,
				InterruptedException {
			long sum = 0;
			for (LongWritable value : values) {
				sum += value.get();
			}
			context.write(key, new LongWritable(sum));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(AggregateV3.class);
		job.setJobName("AggregateV3");

		job.setNumReduceTasks(2);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);

		job.setCombinerClass(SumReducer.class);
		job.setReducerClass(SumReducer.class);

		job.setSortComparatorClass(LongWritable.DecreasingComparator.class);

		MultipleInputs.addInputPath(job, new Path("/geekple/input/emp"), TextInputFormat.class, EmploeeMapper.class);
		FileOutputFormat.setOutputPath(job, new Path("/geekple/output/aggregate/v3"));

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new AggregateV3(), args);
		System.exit(exitCode);
	}
}
