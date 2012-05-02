package com.geekple.fun.aggregate;

import java.util.LinkedHashMap;
import java.util.Map;

import com.geekple.fun.MapReduceTestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Test;

/**
 * @author Daegeun Kim
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class TestAggregate extends MapReduceTestCase {
	private Log LOG = LogFactory.getLog(TestAggregate.class);
	
	@Test
	public void aggregate() throws Exception {
		Map<Mapper, Reducer> mapreduces = new LinkedHashMap<Mapper, Reducer>();
		mapreduces.put(new AggregateV1.EmploeeMapper(), new AggregateV1.SumReducer());
		mapreduces.put(new AggregateV2.EmploeeMapper(), new AggregateV2.SumReducer());
		mapreduces.put(new AggregateV3.EmploeeMapper(), new AggregateV3.SumReducer());
		
		for (Map.Entry<Mapper, Reducer> entry : mapreduces.entrySet()) {
			LOG.info("# start map/reduce : " + entry.getKey().getClass().getName());
			
			MapReduceDriver driver = new MapReduceDriver(entry.getKey(), entry.getValue());
			driver.setKeyGroupingComparator(new LongWritable.DecreasingComparator());
			
			withInputFromEmploees(driver);
			
			then(driver);
		}
	}

	private void then(MapReduceDriver driver) {
		driver.withOutput(new LongWritable(30), new LongWritable(9400));
		driver.withOutput(new LongWritable(20), new LongWritable(10875));
		driver.withOutput(new LongWritable(10), new LongWritable(8750));
		driver.runTest();
	}
}
