package com.geekple.fun.ex.common;

import static org.hamcrest.CoreMatchers.is;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.geekple.fun.Description;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Daegeun Kim
 */
public class TestKey {
	@Test
	@Description("조인조건이 하나일 때 정렬되는 지 검사. 들어온 순서와는 상관없이 alias 도 정렬")
	public void testSort_SingleField() throws Exception {
		List<Key> keys = new ArrayList<Key>();
		keys.addAll(given("b", new String[][] {
				{"20"}, {"10"}, {"30"}, {"40"}, {"50"}, {"10"}, {"20"}
		}));
		keys.addAll(given("a", new String[][] {
				{"10"}, {"20"}, {"30"}, {"40"}, {"50"}
		}));
		Collections.sort(keys, new Key.SortComparator());
		Assert.assertThat(composeValues(keys), is("10:a,10:b,10:b,20:a,20:b,20:b,30:a,30:b,40:a,40:b,50:a,50:b"));
	}

	@Test
	@Description("조인조건이 두개이상일 때 정렬되는지 검사. 들어온 순서와는 상관없이 alias 도 정렬")
	public void testSort_MultipleFields() throws Exception {
		List<Key> keys = new ArrayList<Key>();
		keys.addAll(given("b", new String[][] {
				{"10", "B"}, {"30", "A"}, {"40", "A"}, {"50", "B"}, {"10", "A"}, {"50", "A"}, {"30", "B"}, {"20", "A"}, {"40", "B"}, {"20", "B"}
		}));
		keys.addAll(given("a", new String[][] {
				{"10", "A"}, {"20", "A"}, {"50", "A"}, {"30", "B"}, {"10", "B"}, {"20", "B"}, {"30", "A"}, {"40", "B"}, {"40", "A"}, {"50", "B"}
		}));
		Collections.sort(keys, new Key.SortComparator());
		
		String expectedValue = "";
		for (int i = 10; i <= 50; i+=10) {
			for (char c = 'A'; c <= 'B'; c++) {
				expectedValue += i == 10 && c == 'A' ? "" : ",";
				expectedValue += i + "," + c + ":a";
				expectedValue += ",";
				expectedValue += i + "," + c + ":b";
			}
		}
		Assert.assertThat(composeValues(keys), is(expectedValue));
	}

	@SuppressWarnings("unchecked")
	@Test
	@Description("조인조건이 하나일 때 alias 제외하고 정렬되는지 검사. alias 같을 때는 들어온 순서로 정렬")
	public void testGrouping_SingleField() throws Exception {
		List<Key> keys = new ArrayList<Key>();
		keys.addAll(given("b", new String[][] {
				{"20"}, {"10"}, {"30"}, {"40"}, {"50"}, {"10"}, {"20"}
		}));
		keys.addAll(given("a", new String[][] {
				{"10"}, {"20"}, {"30"}, {"40"}, {"50"}
		}));
		Collections.sort(keys, new Key.GroupingComparator());
		Assert.assertThat(composeValues(keys), is("10:b,10:b,10:a,20:b,20:b,20:a,30:b,30:a,40:b,40:a,50:b,50:a"));
	}
	
	@SuppressWarnings("unchecked")
	@Test
	@Description("조인조건이 두개이상일 때 alias 제외하고 정렬되는지 검사. alias 같을 때는 들어온 순서로 정렬")
	public void testGrouping_MultipleFields() throws Exception {
		List<Key> keys = new ArrayList<Key>();
		keys.addAll(given("b", new String[][] {
				{"10", "B"}, {"30", "A"}, {"40", "A"}, {"50", "B"}, {"10", "A"}, {"50", "A"}, {"30", "B"}, {"20", "A"}, {"40", "B"}, {"20", "B"}
		}));
		keys.addAll(given("a", new String[][] {
				{"10", "A"}, {"20", "A"}, {"50", "A"}, {"30", "B"}, {"10", "B"}, {"20", "B"}, {"30", "A"}, {"40", "B"}, {"40", "A"}, {"50", "B"}
		}));
		Collections.sort(keys, new Key.GroupingComparator());

		String expectedValue = "";
		for (int i = 10; i <= 50; i+=10) {
			for (char c = 'A'; c <= 'B'; c++) {
				expectedValue += i == 10 && c == 'A' ? "" : ",";
				expectedValue += i + "," + c + ":b";
				expectedValue += ",";
				expectedValue += i + "," + c + ":a";
			}
		}
		Assert.assertThat(composeValues(keys), is(expectedValue));
	}
	
	private Collection<Key> given(String alias, String[][] values) {
		List<Key> list = new ArrayList<Key>();
		for (String[] value : values) {
			list.add(addKey(alias, value));
		}
		return list;
	}
	
	private Key addKey(String alias, String... keys) {
		@SuppressWarnings("rawtypes")
		WritableComparable[] writables = new WritableComparable[keys.length];
		int current = 0;
		for (String k : keys) {
			writables[current++] = new Text(k);
		}
		return new Key(alias, writables);
	}
	
	private String composeValues(List<Key> keys) {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (Key key : keys) {
			sb.append(first ? "" : ",");
			sb.append(key).append(":").append(key.alias);
			first = false;
		}
		return sb.toString();
	}
}
