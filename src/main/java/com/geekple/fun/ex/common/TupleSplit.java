package com.geekple.fun.ex.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * FIleSplit 과 거의 동일하며 serialization/deserialization 할 때 alias 를 추가.
 * @author Daegeun Kim
 */
public class TupleSplit extends FileSplit {
	private String alias;
	
	public TupleSplit() {
		super();
	}
	
	public TupleSplit(String alias, Path file, long start, long length, String[] hosts) {
		super(file, start, length, hosts);
		this.alias = alias;
	}
	
	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		alias = WritableUtils.readString(in);
		super.readFields(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, alias);
		super.write(out);
	}
}
