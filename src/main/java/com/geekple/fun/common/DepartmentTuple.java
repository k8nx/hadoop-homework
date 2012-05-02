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
public class DepartmentTuple {
public static final int DEPTNO = 0;
public static final int NAME = 1;
public static final int LOC = 2;

public static Tuple parse(int placing, String record) {
	List<Writable> writables = new ArrayList<Writable>();
	StringTokenizer tokenizer = new StringTokenizer(record);
	int current = 0;
	while (tokenizer.hasMoreTokens()) {
		switch (current++) {
		case DEPTNO: writables.add(new LongWritable(TypeUtils.parseLong(tokenizer.nextToken()))); break;
		case NAME: writables.add(new Text(tokenizer.nextToken())); break;
		case LOC: writables.add(new Text(tokenizer.nextToken())); break;
		}
	}
	return new Tuple(placing, writables.toArray(new Writable[0]));
}
}