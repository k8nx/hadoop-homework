package com.geekple.fun.ex.common;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper 하는 일 없음 바로 통과
 * 
 * @author Daegeun Kim
 */
public class TupleMapper extends Mapper<Key, TupleEx, Key, TupleEx> {
	protected void map(Key key, TupleEx value, Mapper<LongWritable, TupleEx, Key, TupleEx>.Context context)
			throws IOException, InterruptedException {
		context.write(key, value);
	}
}
