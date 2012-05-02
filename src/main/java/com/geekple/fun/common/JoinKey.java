package com.geekple.fun.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * @author Daegeun Kim
 */
public class JoinKey implements WritableComparable<JoinKey> {
	public static final int LEFT = 1;
	public static final int RIGHT = 2;
	
	private int placing;
	private int departmentNo;

	public JoinKey() {
	}
	
	public JoinKey(long departmentNo, int placing) {
		this.placing = placing;
		this.departmentNo = (int) departmentNo;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof JoinKey)) {
			return false;
		}
		return obj != null && departmentNo == ((JoinKey) obj).departmentNo;
	}
	
	@Override
	public int hashCode() {
		return departmentNo;
	}
	
	@Override
	public String toString() {
		return departmentNo + " : " + placing;
	}

	@Override
	public int compareTo(JoinKey o) {
		JoinKey that = o;
		return departmentNo < that.departmentNo ? -1 : (departmentNo == that.departmentNo ? 0 : 1);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVInt(out, departmentNo);
		WritableUtils.writeVInt(out, placing);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		departmentNo = WritableUtils.readVInt(in);
		placing = WritableUtils.readVInt(in);
	}
	
	public static class GroupComparator extends WritableComparator {
		public GroupComparator() {
			super(JoinKey.class, true);
		}
		
		@Override
		public int compare(Object a, Object b) {
			JoinKey thi = (JoinKey) a;
			JoinKey that = (JoinKey) b;
			return thi.departmentNo < that.departmentNo ? -1 : (thi.departmentNo == that.departmentNo ? 0 : 1);
		}
	}

	public static class SortComparator implements RawComparator<JoinKey> {
		@Override
		public int compare(JoinKey o1, JoinKey o2) {
			JoinKey thi = o1;
			JoinKey that = o2;
			if (that.departmentNo == thi.departmentNo) {
				return thi.placing < that.placing ? -1 : (thi.placing == that.placing ? 0 : 1);
			}
			return thi.departmentNo < that.departmentNo ? -1 : (thi.departmentNo == that.departmentNo ? 0 : 1);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			DataInputBuffer buf = new DataInputBuffer();
			try {
				buf.reset(b1, s1, l1);
				JoinKey thi = new JoinKey(WritableUtils.readVInt(buf), WritableUtils.readVInt(buf));
				buf.reset(b2, s2, l2);
				JoinKey that = new JoinKey(WritableUtils.readVInt(buf), WritableUtils.readVInt(buf));
				return compare(thi, that);
			} catch (IOException e) {
				e.printStackTrace();
			}
			return 0;
		}
	}
	
	static {
		WritableComparator.define(JoinKey.class, new GroupComparator());
	}
}
