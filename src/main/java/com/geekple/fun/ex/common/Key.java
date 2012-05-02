package com.geekple.fun.ex.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.geekple.fun.ex.common.TupleEx.Field;
import com.geekple.fun.ex.common.TupleEx.FieldSchema;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * @author Daegeun Kim
 */
@SuppressWarnings("rawtypes")
public class Key implements WritableComparable {
	String alias;
	WritableComparable[] fields;
	
	public Key() {
	}

	public Key(String alias, WritableComparable[] fields) {
		this.alias = alias;
		this.fields = fields;
	}
	
	@Override
	public int hashCode() {
		int hash = 0;
		for (int i = 0; i < fields.length; i++) {
			hash = 163 * hash + fields[i].hashCode();
		}
		return hash;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (obj == this) {
			return true;
		}
		for (int i = 0; i < fields.length; i++) {
			if (!fields[i].equals(((Key) obj).fields[i])) {
				return false;
			}
		}
		return true;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (Writable value : fields) {
			sb.append(first ? "" : ",").append(value);
			first = false;
		}
		return sb.toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		alias = WritableUtils.readString(in);
		// key definition 정보를 보내는 것은 reducer 가 돌기전에 setup 에서 해결할 수 없어서 임.
		// shuffle 전에 할 수 있는 방법이 있다면 이 부분은 불필요.
		String definition = WritableUtils.readString(in);
		TupleEx.registerFieldSchema("$K", definition);
		FieldSchema schema = TupleEx.getFieldSchema("$K");
		WritableComparable[] values = new WritableComparable[schema.size()];
		int current = 0;
		for (Field field : schema) {
			values[current++] = (WritableComparable) field.invoke(in);
		}
		this.fields = values;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, alias);
		TupleEx.getFieldSchema("$K").write(out);
		for (Writable value : fields) {
			value.write(out);
		}
	}
	
	@Override
	public int compareTo(Object o) {
		Key o1 = this;
		Key o2 = (Key) o;
		if (o1 == o2) {
			return 0;
		}
		if (o1.fields.length == o2.fields.length) {
			for (int i = 0; i < o1.fields.length; i++) {
				int gt = WritableComparator.get(o1.fields[i].getClass()).compare(o1.fields[i], o2.fields[i]);
				if (gt != 0) {
					return gt;
				}
			}
		}
		return 0;
	}
	
	public static class SortComparator implements RawComparator<Key> {
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			DataInputBuffer buf = new DataInputBuffer();
			try {
				buf.reset(b1, s1, l1);
				Key thi = new Key();
				thi.readFields(buf);
				buf.reset(b2, s2, l2);
				Key that = new Key();
				that.readFields(buf);
				return compare(thi, that);
			} catch (IOException e) {
				e.printStackTrace();
			}
			return 0;
		}
		
		@Override
		public int compare(Key o1, Key o2) {
			if (o1 == o2) {
				return 0;
			}
			if (o1.fields.length == o2.fields.length) {
				for (int i = 0; i < o1.fields.length; i++) {
					int gt = WritableComparator.get(o1.fields[i].getClass()).compare(o1.fields[i], o2.fields[i]);
					if (gt != 0) {
						return gt;
					}
				}
				return o1.alias.compareTo(o2.alias);
			}
			return 0;
		}
	}
	
	public static class GroupingComparator extends WritableComparator {
		// 굳이 생성해서 비교할 필요는 없지만 일단 여기서 패스
		protected GroupingComparator() {
			super(Key.class, true);
		}
		
		@Override
		public int compare(Object a, Object b) {
			Key o1 = (Key) a;
			Key o2 = (Key) b;
			if (o1 == o2) {
				return 0;
			}
			if (o1.fields.length == o2.fields.length) {
				for (int i = 0; i < o1.fields.length; i++) {
					int gt = WritableComparator.get(o1.fields[i].getClass()).compare(o1.fields[i], o2.fields[i]);
					if (gt != 0) {
						return gt;
					}
				}
			}
			return 0;
		}
	}
	
	static {
		WritableComparator.define(Key.class, new GroupingComparator());
	}
}
