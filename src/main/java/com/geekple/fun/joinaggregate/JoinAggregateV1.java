package com.geekple.fun.joinaggregate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import com.geekple.fun.aggregate.AggregateV1;
import com.geekple.fun.common.DepartmentTuple;
import com.geekple.fun.common.EmploeeTuple;
import com.geekple.fun.common.JoinKey;
import com.geekple.fun.common.Tuple;
import com.geekple.fun.join.JoinV1;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * DistributedCache 를 활용한 방법
 * <pre>
 * 1. 1번 과제를 그대로 이용
 * 2. 1단계에서 나온 결과 디렉토리를 DistributedCache 로 기록하고
 * 3. Reduce 단계에서 지정한 디렉토리 하위에 part- 파일을 읽어서 메모리에 기록한다
 * 4. Join 한 후 total salary 값을 포함시킨다.
 * </pre>
 * 
 * @author Daegeun Kim
 */
@SuppressWarnings("deprecation")
public class JoinAggregateV1 extends Configured implements Tool {
	private static final Log LOG = LogFactory.getLog(JoinAggregateV1.class);

	public static class JoinReducer extends Reducer<JoinKey, Tuple, LongWritable, Tuple> {
		private Map<Long, Long> totalSalaryByDepartment = new HashMap<Long, Long>();
		
		protected void setup(Reducer<JoinKey,Tuple,LongWritable,Tuple>.Context context) throws IOException ,InterruptedException {
			super.setup(context);
			URI[] files = DistributedCache.getCacheFiles(context.getConfiguration());
			if (files != null && files.length > 0) {
				Path path = new Path(files[0].toString());
				FileSystem fs = path.getFileSystem(context.getConfiguration());
				FileStatus[] statuses = fs.listStatus(path);
				for (FileStatus status : statuses) {
					if (status.getPath().getName().startsWith("part-") && status.getLen() > 0) {
						BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(status.getPath())));
						String line = in.readLine();
						while (line != null) {
							StringTokenizer tokens = new StringTokenizer(line);
							if (tokens.hasMoreTokens()) {
								try {
									LOG.info("cached : " + line);
									totalSalaryByDepartment.put(Long.parseLong(tokens.nextToken()), Long.parseLong(tokens.nextToken()));
								} catch (Exception e) {
									LOG.info("text is incomplete. - " + line);
								}
							}
							line = in.readLine();
						}
					}
				}
			}
		}
		
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
							new LongWritable(totalSalaryByDepartment.get(emploee.getField(LongWritable.class, EmploeeTuple.DEPTNO).get()))
					));
				}
			}
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Job firstJob = new Job(getConf());
		firstJob.setJobName("Aggregate");
		firstJob.setJarByClass(JoinAggregateV1.class);
		
		firstJob.setMapperClass(AggregateV1.EmploeeMapper.class);
		firstJob.setReducerClass(AggregateV1.SumReducer.class);
		
		firstJob.setMapOutputKeyClass(LongWritable.class);
		firstJob.setMapOutputValueClass(LongWritable.class);
		
		firstJob.setOutputKeyClass(LongWritable.class);
		firstJob.setOutputValueClass(LongWritable.class);
		
		firstJob.setSortComparatorClass(LongWritable.DecreasingComparator.class);

		Path saved = new Path("/geekple/output/jaggregate/cached");
		FileInputFormat.setInputPaths(firstJob, new Path("/geekple/input/emp"));
		FileOutputFormat.setOutputPath(firstJob, saved);
		boolean success = firstJob.waitForCompletion(true);
		
		if (success) {
			Job job = new Job(getConf());
			job.setJarByClass(JoinAggregateV1.class);
			job.setJobName("Join");
			
			job.setMapOutputKeyClass(JoinKey.class);
			job.setMapOutputValueClass(Tuple.class);

			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Tuple.class);
			
			job.setOutputFormatClass(TextOutputFormat.class);

			job.setGroupingComparatorClass(JoinKey.GroupComparator.class);
			job.setSortComparatorClass(JoinKey.SortComparator.class);
			
			job.setReducerClass(JoinReducer.class);
			
			MultipleInputs.addInputPath(job, new Path("/geekple/input/emp"), TextInputFormat.class, JoinV1.EmploeeMapper.class);
			MultipleInputs.addInputPath(job, new Path("/geekple/input/dept"), TextInputFormat.class, JoinV1.DepartmentMapper.class);
			FileOutputFormat.setOutputPath(job, new Path("/geekple/output/jaggregate/v1"));
			DistributedCache.addCacheFile(saved.toUri(), job.getConfiguration());
			job.waitForCompletion(true);
		}
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new JoinAggregateV1(), args);
		System.exit(exitCode);
	}
}
