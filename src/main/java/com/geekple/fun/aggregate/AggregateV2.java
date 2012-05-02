package com.geekple.fun.aggregate;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
 * 불필요한 parse 작업 제거하고 간소화한 버전
 * @author Daegeun Kim
 */
@SuppressWarnings("deprecation")
public class AggregateV2 extends Configured implements Tool {
	private static final Log LOG = LogFactory.getLog(AggregateV2.class);
	
	public static class EmploeeMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, LongWritable, LongWritable>.Context context) throws IOException,
				InterruptedException {
			try {
				StringTokenizer tokens = new StringTokenizer(value.toString());
				int current = 0;
				long departmentNo = -1;
				long salary = -1;
				while (tokens.hasMoreTokens()) {
					switch (current++) {
					case 5: salary = Long.parseLong(tokens.nextToken()); break;
					case 7: departmentNo = Long.parseLong(tokens.nextToken()); break;
					default: tokens.nextToken(); break;
					}
				}
				if (departmentNo != -1 && salary != -1) {
					context.write(new LongWritable(departmentNo), new LongWritable(salary));
				} else {
					LOG.info("text is incomplete. - " + value.toString());
				}
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
			}
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
		job.setJarByClass(AggregateV2.class);
		job.setJobName("AggregateV2");
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);

		job.setReducerClass(SumReducer.class);

		job.setSortComparatorClass(LongWritable.DecreasingComparator.class);

		MultipleInputs.addInputPath(job, new Path("/geekple/input/emp"), TextInputFormat.class, EmploeeMapper.class);
		FileOutputFormat.setOutputPath(job, new Path("/geekple/output/aggregate/v2"));

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new AggregateV2(), args);
		System.exit(exitCode);
	}
}
