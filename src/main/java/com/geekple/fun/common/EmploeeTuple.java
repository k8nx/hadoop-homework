package com.geekple.fun.common;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import com.geekple.fun.util.TypeUtils;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * @author Daegeun Kim
 */
public class EmploeeTuple {
	public static final int EMPNO = 0;
	public static final int NAME = 1;
	public static final int JOB = 2;
	public static final int MGR = 3;
	public static final int HIREDATE = 4;
	public static final int SAL = 5;
	public static final int COMM = 6;
	public static final int DEPTNO = 7;
	
	public static Tuple parse(String record) {
		return parse(0, record);
	}
	
	public static Tuple parse(int placing, String record) {
		List<Writable> writables = new ArrayList<Writable>();
		StringTokenizer tokenizer = new StringTokenizer(record);
		int current = 0;
		while (tokenizer.hasMoreTokens()) {
			switch (current++) {
			case EMPNO: writables.add(new LongWritable(TypeUtils.parseLong(tokenizer.nextToken()))); break;
			case NAME: writables.add(new Text(tokenizer.nextToken())); break;
			case JOB: writables.add(new Text(tokenizer.nextToken())); break;
			case SAL: writables.add(new LongWritable(TypeUtils.parseLong(tokenizer.nextToken()))); break;
			case DEPTNO: writables.add(new LongWritable(TypeUtils.parseLong(tokenizer.nextToken()))); break;
			default: writables.add(new Text(tokenizer.nextToken())); break;
			}
		}
		return new Tuple(placing, writables.toArray(new Writable[0]));
	}
}
