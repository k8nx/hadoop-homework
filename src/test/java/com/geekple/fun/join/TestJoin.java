package com.geekple.fun.join;

import java.util.List;

import com.geekple.fun.Description;
import com.geekple.fun.MapReduceTestCase;
import com.geekple.fun.common.JoinKey;
import com.geekple.fun.common.Tuple;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

/**
 * @author Daegeun Kim
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class TestJoin extends MapReduceTestCase {
	@Test
	@Description("join 문제")
	public void join() throws Exception {
		List<Pair> pairs = multipleInputs();
		
		MapReduceDriver driver = new MapReduceDriver(getByPassMapper(), new JoinV1.JoinReducer());
		driver.setKeyOrderComparator(new JoinKey.SortComparator());
		for (Pair pair : pairs) {
			driver.withInput(pair.getFirst(), pair.getSecond());
		}
		
		then(driver);
	}

	private void then(MapReduceDriver driver) {
		driver.withOutput(new LongWritable(7782), new Tuple(0, new Text("CLARK"), new Text("ACCOUNTING"), new LongWritable(2450)));
		driver.withOutput(new LongWritable(7839), new Tuple(0, new Text("KING"), new Text("ACCOUNTING"), new LongWritable(5000)));
		driver.withOutput(new LongWritable(7934), new Tuple(0, new Text("MILLER"), new Text("ACCOUNTING"), new LongWritable(1300)));
		driver.withOutput(new LongWritable(7369), new Tuple(0, new Text("SMITH"), new Text("RESEARCH"), new LongWritable(800)));
		driver.withOutput(new LongWritable(7566), new Tuple(0, new Text("JONES"), new Text("RESEARCH"), new LongWritable(2975)));
		driver.withOutput(new LongWritable(7788), new Tuple(0, new Text("SCOTT"), new Text("RESEARCH"), new LongWritable(3000)));
		driver.withOutput(new LongWritable(7876), new Tuple(0, new Text("ADAMS"), new Text("RESEARCH"), new LongWritable(1100)));
		driver.withOutput(new LongWritable(7902), new Tuple(0, new Text("FORD"), new Text("RESEARCH"), new LongWritable(3000)));
		driver.withOutput(new LongWritable(7499), new Tuple(0, new Text("ALLEN"), new Text("SALES"), new LongWritable(1600)));
		driver.withOutput(new LongWritable(7521), new Tuple(0, new Text("WARD"), new Text("SALES"), new LongWritable(1250)));
		driver.withOutput(new LongWritable(7654), new Tuple(0, new Text("MARTIN"), new Text("SALES"), new LongWritable(1250)));
		driver.withOutput(new LongWritable(7698), new Tuple(0, new Text("BLAKE"), new Text("SALES"), new LongWritable(2850)));
		driver.withOutput(new LongWritable(7844), new Tuple(0, new Text("TURNER"), new Text("SALES"), new LongWritable(1500)));
		driver.withOutput(new LongWritable(7900), new Tuple(0, new Text("JAMES"), new Text("SALES"), new LongWritable(950)));
		driver.runTest();
	}
}
