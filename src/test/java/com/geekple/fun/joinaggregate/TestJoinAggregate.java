package com.geekple.fun.joinaggregate;

import java.util.List;

import com.geekple.fun.MapReduceTestCase;
import com.geekple.fun.aggregate.AggregateV1;
import com.geekple.fun.common.JoinKey;
import com.geekple.fun.common.Tuple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

/**
 * @author Daegeun Kim
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class TestJoinAggregate extends MapReduceTestCase {
	static {
		Configuration.addDefaultResource("core-site.xml");
		Configuration.addDefaultResource("mapred-site.xml");
	}
	Configuration configuration = new Configuration();
	
	protected Configuration getConf() {
		return configuration;
	}
	
	@Test
	public void joinaggreate() throws Exception {
		FileSystem fs = FileSystem.get(getConf());
		Path path = new Path("/geekple/input/cached/part-0000");
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
		FSDataOutputStream out = fs.create(path);
		MapReduceDriver step1 = new MapReduceDriver(new AggregateV1.EmploeeMapper(), new AggregateV1.SumReducer());
		withInputFromEmploees(step1);
		for (Pair<LongWritable, LongWritable> pair : (List<Pair>) step1.run()) {
			out.write(String.format("%d\t%d\n", pair.getFirst().get(), pair.getSecond().get()).getBytes());
		}
		out.flush();
		out.close();
		DistributedCache.addCacheFile(new Path("/geekple/input/cached").toUri(), getConf());
		
		List<Pair> pairs = multipleInputs();
		
		MapReduceDriver driver = new MapReduceDriver(getByPassMapper(), new JoinAggregateV1.JoinReducer());
		driver.setConfiguration(getConf());
		driver.setKeyOrderComparator(new JoinKey.SortComparator());
		for (Pair pair : pairs) {
			driver.withInput(pair.getFirst(), pair.getSecond());
		}
		
		then(driver);
	}

	private void then(MapReduceDriver driver) {
		driver.withOutput(new LongWritable(7782), new Tuple(0, new Text("CLARK"), new Text("ACCOUNTING"), new LongWritable(8750)));
		driver.withOutput(new LongWritable(7839), new Tuple(0, new Text("KING"), new Text("ACCOUNTING"), new LongWritable(8750)));
		driver.withOutput(new LongWritable(7934), new Tuple(0, new Text("MILLER"), new Text("ACCOUNTING"), new LongWritable(8750)));
		driver.withOutput(new LongWritable(7369), new Tuple(0, new Text("SMITH"), new Text("RESEARCH"), new LongWritable(10875)));
		driver.withOutput(new LongWritable(7566), new Tuple(0, new Text("JONES"), new Text("RESEARCH"), new LongWritable(10875)));
		driver.withOutput(new LongWritable(7788), new Tuple(0, new Text("SCOTT"), new Text("RESEARCH"), new LongWritable(10875)));
		driver.withOutput(new LongWritable(7876), new Tuple(0, new Text("ADAMS"), new Text("RESEARCH"), new LongWritable(10875)));
		driver.withOutput(new LongWritable(7902), new Tuple(0, new Text("FORD"), new Text("RESEARCH"), new LongWritable(10875)));
		driver.withOutput(new LongWritable(7499), new Tuple(0, new Text("ALLEN"), new Text("SALES"), new LongWritable(9400)));
		driver.withOutput(new LongWritable(7521), new Tuple(0, new Text("WARD"), new Text("SALES"), new LongWritable(9400)));
		driver.withOutput(new LongWritable(7654), new Tuple(0, new Text("MARTIN"), new Text("SALES"), new LongWritable(9400)));
		driver.withOutput(new LongWritable(7698), new Tuple(0, new Text("BLAKE"), new Text("SALES"), new LongWritable(9400)));
		driver.withOutput(new LongWritable(7844), new Tuple(0, new Text("TURNER"), new Text("SALES"), new LongWritable(9400)));
		driver.withOutput(new LongWritable(7900), new Tuple(0, new Text("JAMES"), new Text("SALES"), new LongWritable(9400)));
		driver.runTest();
	}
}
