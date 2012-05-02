package com.geekple.fun;

import java.io.IOException;

import com.geekple.fun.common.Tuple;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * 깔끔한 Test Code 를 위한 Helper 클래스
 * @author Daegeun Kim
 */
public abstract class TestCaseSupport {
	protected static final int DEFAULT_RANDOM_TUPLE_SIZE = 4;
	
	protected Tuple createRandomTuple() {
		String[] names = {"Android", "iPhone", "iPad", "iPod", "BlackBerry"};
		Tuple tuple = new Tuple(0, 
				new Text(names[(int) (Math.random() * (names.length - 1))]), 
				new IntWritable((int) (Math.random() * 100)),
				new Text(""),
				new Text("")
		);
		return tuple;
	}
	
	protected ByteArrayDataOutput writeToStream(Writable writable) throws IOException {
		ByteArrayDataOutput out = new ByteArrayDataOutput();
		writable.write(out);
		return out;
	}

	protected ByteArrayDataInput readFromStream(ByteArrayDataOutput out) {
		return new ByteArrayDataInput(out.getBytes());
	}
}
