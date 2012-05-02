package com.geekple.fun.common;

import static org.hamcrest.CoreMatchers.is;

import com.geekple.fun.Description;
import com.geekple.fun.ByteArrayDataOutput;
import com.geekple.fun.TestCaseSupport;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tuple Test
 * 쓰다가 불필요한 유닛 테스트는 삭제를 하지만 지금 볼 때 필요없어보이는 것이 있는데 그것은 일부러 삭제하지 않은 것임.
 * 
 * @author Daegeun Kim
 */
public class TestTuple extends TestCaseSupport {
	@Test
	@Description("Tuple 생성")
	public void testInit() {
		Tuple tuple = createRandomTuple();
		Assert.assertThat(tuple.size(), is(DEFAULT_RANDOM_TUPLE_SIZE));
	}
	
	@Test
	@Description("readFields & write 테스트")
	public void testReadWrite() throws Exception {
		Tuple tuple = createRandomTuple();
		ByteArrayDataOutput out = writeToStream(tuple);
		
		Tuple newtuple = new Tuple();
		newtuple.readFields(readFromStream(out));

		Assert.assertThat(newtuple.size(), is(tuple.size()));
		Assert.assertThat(newtuple.getField(Text.class, 0), is(tuple.getField(Text.class, 0)));
		Assert.assertThat(newtuple.getField(IntWritable.class, 1), is(tuple.getField(IntWritable.class, 1)));
	}
}
