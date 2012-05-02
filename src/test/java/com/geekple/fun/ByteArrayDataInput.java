package com.geekple.fun;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

/**
 * @author Daegeun Kim
 */
public class ByteArrayDataInput extends DataInputStream {
	public ByteArrayDataInput(byte[] bytes) {
		super(new ByteArrayInputStream(bytes));
	}
}
