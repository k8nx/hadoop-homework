package com.geekple.fun.util;


/**
 * @author Daegeun Kim
 */
public class TypeUtils {
	public static long parseLong(String value) {
		return parseLong(value, 0L);
	}
	
	public static int parseInt(String value) {
		return parseInt(value, 0);
	}
	
	public static long parseLong(String value, long defaultValue) {
		try {
			return Long.parseLong(value);
		} catch (Exception e) {
			return defaultValue;
		}
	}
	
	public static int parseInt(String value, int defaultValue) {
		try {
			return Integer.parseInt(value);
		} catch (Exception e) {
			return defaultValue;
		}
	}
}
