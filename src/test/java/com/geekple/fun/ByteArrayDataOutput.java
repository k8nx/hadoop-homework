package com.geekple.fun;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

/**
 * @author Daegeun Kim
 */
public class ByteArrayDataOutput extends DataOutputStream {
	public ByteArrayDataOutput() {
		super(new ByteArrayOutputStream());
	}
	
	public byte[] getBytes() {
		return ((ByteArrayOutputStream) out).toByteArray();
	}
}
