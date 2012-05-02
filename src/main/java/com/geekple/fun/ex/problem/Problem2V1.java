package com.geekple.fun.ex.problem;

import com.geekple.fun.aggregate.AggregateV3;
import com.geekple.fun.ex.common.Key;
import com.geekple.fun.ex.common.TupleEx;
import com.geekple.fun.ex.common.TupleInnerJoinReducer;
import com.geekple.fun.ex.common.TupleInputFormat;
import com.geekple.fun.ex.common.TupleMapper;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Daegeun Kim
 */
@SuppressWarnings("deprecation")
public class Problem2V1 extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		FileSystem.get(getConf()).delete(new Path("/geekple/input/sum"), true);
		
		Job prejob = new Job(getConf());
		prejob.setJarByClass(Problem2V1.class);
		prejob.setMapperClass(AggregateV3.EmploeeMapper.class);
		prejob.setReducerClass(AggregateV3.SumReducer.class);
		prejob.setInputFormatClass(TextInputFormat.class);
		prejob.setMapOutputKeyClass(LongWritable.class);
		prejob.setMapOutputValueClass(LongWritable.class);
		
		TextInputFormat.addInputPath(prejob, new Path("/geekple/input/emp"));
		TextOutputFormat.setOutputPath(prejob, new Path("/geekple/input/sum"));
		prejob.waitForCompletion(true);
		
		Job job = new Job(getConf());
		job.setJarByClass(Problem2V1.class);
		job.setMapOutputKeyClass(Key.class);
		job.setMapOutputValueClass(TupleEx.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(TupleEx.class);
		
		job.setSortComparatorClass(Key.SortComparator.class);
		job.setGroupingComparatorClass(Key.GroupingComparator.class);
		
		job.setInputFormatClass(TupleInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(TupleMapper.class);
		job.setReducerClass(TupleInnerJoinReducer.class);
		
		/*
		 * 제약사항 및 권고사항
		 * 1. 현재는 int, chararray, float type 만 지원. type 을 지정하지 않으면 chararray 가 기본값.
		 * 2. addTable 모두 한 후 setJoinExpression, setProjections 메서드를 사용해야함. 
		 * 3. $K, $P 는 예약 alias 입니다. 사용하면 안됨.
		 * 4. JoinExpression 을 사용할 때 되도록이면 앞에 작성된 table 의 tuple 이 적은 순서로 작성권고. 메모리 낭비됨.
		 * 5. JoinExpression 에 다중 필드 조건을 줄 수 있으나 필드타입 및 개수가 일치하여야 함.
		 * 6. Projections 작성시 (조인에 사용할 조건을 제외)  alias와 무관하게 field name 이 같아서는 안됨. 아래처럼 dname, ename 으로 분리한 이유.
		 * 7. 필드네임은 $로 시작하면 index 로 간주하기때문에 사용자제.
		 */
		TupleInputFormat.addTable(job, "A", new Path("/geekple/input/dept"), "deptno:int, dname:chararray, location:chararray");
		TupleInputFormat.addTable(job, "B", new Path("/geekple/input/sum"), "deptno:int, sal:int");
		TupleInputFormat.addTable(job, "C", new Path("/geekple/input/emp"), "empno:int, ename:chararray, job, mgr, hiredate, sal:int, comm, deptno:int");
		TupleInputFormat.setJoinExpression(job, "A by deptno, B by deptno, C by deptno");
		TupleInputFormat.setProjections(job, "C.empno, C.ename, A.dname, B.sal");
		
		FileOutputFormat.setOutputPath(job, new Path("/geekple/output/ex/p2/v1"));
		FileSystem.get(getConf()).delete(new Path("/geekple/output/ex/p2/v1"), true);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Problem2V1(), args);
		System.exit(exitCode);
	}
}
