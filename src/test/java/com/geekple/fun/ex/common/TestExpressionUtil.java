package com.geekple.fun.ex.common;

import static org.hamcrest.CoreMatchers.is;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Daegeun Kim
 */
public class TestExpressionUtil {
	@Test
	public void testParseJoinExpression() throws Exception {
		Assert.assertThat(composeValues(ExpressionUtils.parseJoinExpression("a by deptno, b by id")), is("a(deptno)b(id)"));
		Assert.assertThat(composeValues(ExpressionUtils.parseJoinExpression("     a 	by				 deptno, 				b 	\t	by id")), is("a(deptno)b(id)"));
		Assert.assertThat(composeValues(ExpressionUtils.parseJoinExpression("a by deptno, b by deptno")), is("a(deptno)b(deptno)"));
		Assert.assertThat(composeValues(ExpressionUtils.parseJoinExpression("a by deptno, b by deptno")), is("a(deptno)b(deptno)"));
		Assert.assertThat(composeValues(ExpressionUtils.parseJoinExpression("a by $0, b by $1")), is("a($0)b($1)"));
		Assert.assertThat(composeValues(ExpressionUtils.parseJoinExpression("a       by         $0      ,       b      by    $1     ")), is("a($0)b($1)"));

		Assert.assertThat(composeValues(ExpressionUtils.parseJoinExpression("a by (deptno), b by (id)")), is("a(deptno)b(id)"));
		Assert.assertThat(composeValues(ExpressionUtils.parseJoinExpression("a by (deptno), b by (deptno)")), is("a(deptno)b(deptno)"));
		Assert.assertThat(composeValues(ExpressionUtils.parseJoinExpression("a by ($0), b by ($1)")), is("a($0)b($1)"));

		Assert.assertThat(composeValues(ExpressionUtils.parseJoinExpression("a by deptno,     b \t\t by (id)")), is("a(deptno)b(id)"));
		Assert.assertThat(composeValues(ExpressionUtils.parseJoinExpression("a by deptno, b by (deptno)")), is("a(deptno)b(deptno)"));
		Assert.assertThat(composeValues(ExpressionUtils.parseJoinExpression("a by $0, b     by ($1)")), is("a($0)b($1)"));

		Assert.assertThat(composeValues(ExpressionUtils.parseJoinExpression("a by id, b by aid, c by aid")), is("a(id)b(aid)c(aid)"));
	}

	@Test
	public void testParseProjectionsExpression() throws Exception {
		Assert.assertThat(composeValues(ExpressionUtils.parseProjectionExpression("A.empno, A.name, A.sal, B.name")), is("A(empno,name,sal)B(name)"));
	}
	
	private String composeValues(Map<String, String[]> conditions) {
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, String[]> entry : conditions.entrySet()) {
			sb.append(entry.getKey());
			sb.append("(");
			boolean first = true;
			for (String value : entry.getValue()) {
				sb.append(first ? "" : ",").append(value);
				first = false;
			}
			sb.append(")");
		}
		return sb.toString();
	}
}
