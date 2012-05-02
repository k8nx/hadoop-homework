package com.geekple.fun.ex.common;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * ANTLR, JAVACC 같은 것을 쓰면 좋겠지만 아직은 그런 내공이 아니므로 대충 구현.
 * @author Daegeun Kim
 */
public class ExpressionUtils {
	private static final int ALIAS = 0;
	private static final int BY = 1;
	private static final int IDS = 2;
	private static final int COMMA = 4;
	private static final int COMPLETE = 99;

	/**
	 * 추출할 필드 얻기
	 * @param expression
	 * @return
	 */
	public static Map<String, String[]> parseProjectionExpression(String expression) {
		if (expression == null || expression.length() == 0) {
			return null;
		}
		Map<String, String[]> projections = new LinkedHashMap<String, String[]>();
		for (String value : expression.split("[ ]*,[ ]*")) {
			String[] pair = value.split("\\.");
			String[] original = projections.get(pair[0]);
			if (original == null) {
				projections.put(pair[0], new String[] { pair[1] });
			} else {
				String[] newarray = new String[original.length + 1];
				System.arraycopy(original, 0, newarray, 0, original.length);
				newarray[original.length] = pair[1];
				projections.put(pair[0], newarray);
			}
		}
		return projections;
	}
	
	/**
	 * a.name, b.sal, c.deptname 이 주어졌을 때 ([a, name], [b, sal], [c, deptname]) 반환
	 * 
	 * @param expression
	 * @return
	 */
	public static List<String[]> getProjectionFieldsByExpression(String expression) {
		if (expression == null || expression.length() == 0) {
			return null;
		}
		List<String[]> projections = new ArrayList<String[]>();
		for (String value : expression.split("[ ]*,[ ]*")) {
			String[] pair = value.split("\\.");
			projections.add(new String[] { pair[0], pair[1] });
		}
		return projections;
	}
	
	/**
	 * 조인 조건 필드 얻기
	 * @param expression
	 * @return
	 */
	public static Map<String, String[]> parseJoinExpression(String expression) {
		if (expression == null) {
			throw new IllegalArgumentException("invalid expression - must not be null : " + expression);
		}
		expression = expression.trim();
		Map<String, String[]> conditions = new LinkedHashMap<String, String[]>();
		int expected = 0;
		String alias = null;
		int length = expression.length();
		for (int i = 0; i < length;) {
			int c = i;
			switch (expected) {
			case ALIAS:
				c = expression.indexOf(' ', i);
				alias = expression.substring(i, c); 
				conditions.put(alias, null);
				expected = BY;
				break;
			case BY:
				c = expression.indexOf("by", i) + 2;
				if (!expression.substring(i, c).equals("by")) {
					throw new IllegalArgumentException("invalid expression : " + expression);
				}
				expected = IDS;
				break;
			case IDS:
				if (expression.charAt(i) == '(') {
					c = expression.indexOf(')', i);
					if (c == -1) {
						throw new IllegalArgumentException("invalid expression : " + expression);
					}
					conditions.put(alias, expression.substring(i+1, c++).split("[ ]*,[ ]*"));
				} else {
					c = expression.indexOf(',', i);
					if (c == -1) {
						c = length;
						expected = COMPLETE;
					}
					conditions.put(alias, new String[] { expression.substring(i, c).trim() });
				}
				expected = expected == COMPLETE ? COMPLETE : COMMA;
				break;
			case COMMA:
				c = expression.indexOf(',', i) + 1;
				expected = c == 0 ? COMPLETE : ALIAS;
				break;
			}
			if (c == -1 || expected == COMPLETE) {
				break;
			}
			i = c;
			// consume a whitespaces
			while (i < length) {
				if (Character.isWhitespace(expression.charAt(i))) {
					i++;
				} else {
					break;
				}
			}
		}
		return conditions;
	}
}
