package com.geekple.fun.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Writable 로 null 입력이 불가능한 Tuple 입니다.
 * @author Daegeun Kim
 */
public class Tuple implements Writable {
	private int placing;
	private Writable[] fields;
	
	public Tuple() {
	}
	
	public Tuple(int placing, Writable... fields) {
		this.placing = placing;
		this.fields = fields;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof Tuple)) {
			return false;
		}
		Tuple o = (Tuple) obj;
		if (fields.length != o.fields.length) {
			return false;
		}
		for (int i = 0; i < fields.length; i++) {
			if (!fields[i].equals(o.fields[i])) {
				return false;
			}
		}
		return true;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (Writable field : fields) {
			sb.append(first ? "" : "\t").append(field);
			first = false;
		}
		return sb.toString();
	}
	
	public Writable[] getFields() {
		return fields;
	}
	
	public int getPlacing() {
		return placing;
	}
	
	@SuppressWarnings("unchecked")
	public <T> T getField(Class<? extends T> fieldType, int index) {
		return (T) fields[index];
	}
	
	public void setField(int index, Writable field) {
		fields[index] = field;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int length = WritableUtils.readVInt(in);
		int placing = WritableUtils.readVInt(in);
		List<String> types = new ArrayList<String>();
		for (int i = 0; i < length; i++) {
			types.add(WritableUtils.readString(in));
		}
		Writable[] writables = new Writable[length];
		for (int i = 0; i < length; i++) {
			try {
				writables[i] = (Writable) Class.forName(types.get(i)).newInstance();
				writables[i].readFields(in);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		this.placing = placing;
		this.fields = writables;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVInt(out, fields.length);
		WritableUtils.writeVInt(out, placing);
		for (Writable field : fields) {
			WritableUtils.writeString(out, field.getClass().getName());
		}
		for (Writable field : fields) {
			field.write(out);
		}
	}
	
	public int size() {
		return fields.length;
	}
}
