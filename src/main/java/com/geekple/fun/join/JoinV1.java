package com.geekple.fun.join;

import java.io.IOException;
import java.util.Iterator;

import com.geekple.fun.common.DepartmentTuple;
import com.geekple.fun.common.EmploeeTuple;
import com.geekple.fun.common.JoinKey;
import com.geekple.fun.common.Tuple;

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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Daegeun Kim
 */
@SuppressWarnings("deprecation")
public class JoinV1 extends Configured implements Tool {
	private static final Log LOG = LogFactory.getLog(JoinV1.class);
	
	public static class EmploeeMapper extends Mapper<LongWritable, Text, JoinKey, Tuple> {
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, JoinKey, Tuple>.Context context) throws IOException,
				InterruptedException {
			Tuple tuple = EmploeeTuple.parse(JoinKey.RIGHT, value.toString());
			JoinKey joinKey = new JoinKey(tuple.getField(LongWritable.class, EmploeeTuple.DEPTNO).get(), JoinKey.RIGHT);
			context.write(joinKey, tuple);
		}
	}
	
	public static class DepartmentMapper extends Mapper<LongWritable, Text, JoinKey, Tuple> {
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, JoinKey, Tuple>.Context context) throws IOException,
				InterruptedException {
			Tuple tuple = DepartmentTuple.parse(JoinKey.LEFT, value.toString());
			JoinKey joinKey = new JoinKey(tuple.getField(LongWritable.class, DepartmentTuple.DEPTNO).get(), JoinKey.LEFT);
			context.write(joinKey, tuple);
		}
	}

	public static class JoinReducer extends Reducer<JoinKey, Tuple, LongWritable, Tuple> {
		protected void reduce(JoinKey key, Iterable<Tuple> values,
				Reducer<JoinKey, Tuple, LongWritable, Tuple>.Context context) throws IOException,
				InterruptedException {
			Iterator<Tuple> i = values.iterator();
			Tuple department = i.next();
			Text departmentName = department.getField(Text.class, DepartmentTuple.NAME);
			LOG.info("department: " + department);
			if (department.getPlacing() == JoinKey.LEFT) {
				while (i.hasNext()) {
					Tuple emploee = i.next();
					LOG.info("emploee: " + emploee);
					context.write(emploee.getField(LongWritable.class, EmploeeTuple.EMPNO), new Tuple(0,
							emploee.getField(Text.class, EmploeeTuple.NAME),
							departmentName,
							emploee.getField(LongWritable.class, EmploeeTuple.SAL)
					));
				}
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(JoinV1.class);
		job.setJobName("Join");
		
		job.setMapOutputKeyClass(JoinKey.class);
		job.setMapOutputValueClass(Tuple.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Tuple.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setGroupingComparatorClass(JoinKey.GroupComparator.class);
		job.setSortComparatorClass(JoinKey.SortComparator.class);
		
		job.setReducerClass(JoinReducer.class);
		
		MultipleInputs.addInputPath(job, new Path("/geekple/input/emp"), TextInputFormat.class, EmploeeMapper.class);
		MultipleInputs.addInputPath(job, new Path("/geekple/input/dept"), TextInputFormat.class, DepartmentMapper.class);
		FileOutputFormat.setOutputPath(job, new Path("/geekple/output/join/v1"));

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new JoinV1(), args);
		System.exit(exitCode);
	}
}
