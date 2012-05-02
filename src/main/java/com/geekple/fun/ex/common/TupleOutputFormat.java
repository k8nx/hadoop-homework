package com.geekple.fun.ex.common;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 아직 쓰지 않음.
 * @author Daegeun Kim
 */
public class TupleOutputFormat extends FileOutputFormat<Key, TupleEx> {
	public static void setOutputFormat(Job job, String format) {
		
	}
	
	@Override
	public RecordWriter<Key, TupleEx> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
		return null;
	}
}
