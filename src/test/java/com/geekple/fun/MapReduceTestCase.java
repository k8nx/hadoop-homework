package com.geekple.fun;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.geekple.fun.common.JoinKey;
import com.geekple.fun.common.Tuple;
import com.geekple.fun.ex.common.Key;
import com.geekple.fun.ex.common.TupleEx;
import com.geekple.fun.join.JoinV1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

/**
 * @author Daegeun Kim
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class MapReduceTestCase {
	protected static final String EMP = "7369 SMITH CLERK 7902 1980-12-17 800 NULL 20\n"
			+ "7499 ALLEN SALESMAN 7698 1981-02-20 1600 300 30\n" + "7521 WARD SALESMAN 7698 1981-02-22 1250 500 30\n"
			+ "7566 JONES MANAGER 7839 1981-04-02 2975 NULL 20\n"
			+ "7654 MARTIN SALESMAN 7698 1981-09-28 1250 1400 30\n"
			+ "7698 BLAKE MANAGER 7839 1981-05-01 2850 NULL 30\n" + "7782 CLARK MANAGER 7839 1981-06-09 2450 NULL 10\n"
			+ "7788 SCOTT ANALYST 7566 1987-04-19 3000 NULL 20\n" + "7839 KING PRESIDENT 0 1981-11-17 5000 NULL 10\n"
			+ "7844 TURNER SALESMAN 7698 1981-09-08 1500 0 30\n" + "7876 ADAMS CLERK 7788 1987-05-23 1100 NULL 20\n"
			+ "7900 JAMES CLERK 7698 1981-12-03 950 NULL 30\n" + "7902 FORD ANALYST 7566 1981-12-03 3000 NULL 20\n"
			+ "7934 MILLER CLERK 7782 1982-01-23 1300 NULL 10";

	protected static final String DEPT = "10 ACCOUNTING NEWYORK\n" + "20 RESEARCH DALLAS\n" + "30 SALES CHICAGO\n"
			+ "40 OPERATIONS BOSTON";

	protected List<Pair> multipleInputs() throws IOException {
		List<Pair> pairs = new ArrayList<Pair>();
		MapReduceDriver emploees = new MapReduceDriver(new JoinV1.EmploeeMapper(), getByPassReducer());
		withInputFromEmploees(emploees);
		pairs.addAll(emploees.run());
		
		MapReduceDriver departments= new MapReduceDriver(new JoinV1.DepartmentMapper(), getByPassReducer());
		withInputFromDepartments(departments);
		pairs.addAll(departments.run());
		return pairs;
	}
	
	protected Mapper<JoinKey, Tuple, JoinKey, Tuple> getByPassMapper() {
		return new Mapper<JoinKey, Tuple, JoinKey, Tuple>() {
			protected void map(JoinKey key, Tuple value, Mapper<JoinKey,Tuple,JoinKey,Tuple>.Context context) 
					throws IOException ,InterruptedException {
				context.write(key, value);
			}
		};
	}

	protected Reducer<JoinKey, Tuple, JoinKey, Tuple> getByPassReducer() {
		return new Reducer<JoinKey, Tuple, JoinKey, Tuple>() {
			protected void reduce(JoinKey key, java.lang.Iterable<Tuple> values,
					Reducer<JoinKey, Tuple, JoinKey, Tuple>.Context context)
					throws IOException, InterruptedException {
				for (Tuple tuple : values) {
					context.write(key, tuple);
				}
			}
		};
	}
	
	protected Mapper<Key, TupleEx, Key, TupleEx> getByPassMapperEx() {
		return new Mapper<Key, TupleEx, Key, TupleEx>() {
			protected void map(Key key, TupleEx value, Mapper<Key, TupleEx, Key, TupleEx>.Context context) 
					throws IOException ,InterruptedException {
				context.write(key, value);
			}
		};
	}

	protected Reducer<Key, TupleEx, Key, TupleEx> getByPassReducerEx() {
		return new Reducer<Key, TupleEx, Key, TupleEx>() {
			protected void reduce(Key key, java.lang.Iterable<TupleEx> values,
					Reducer<Key, TupleEx, Key, TupleEx>.Context context)
					throws IOException, InterruptedException {
				for (TupleEx tuple : values) {
					context.write(key, tuple);
				}
			}
		};
	}

	protected void withInputFromEmploees(MapReduceDriver driver) {
		int i = 0;
		for (String line : getEmploeeRecords()) {
			driver.withInput(new LongWritable(i), new Text(line));
			i = nextOffset(i, line);
		}
	}

	protected List<String> getEmploeeRecords() {
		List<String> records = new ArrayList<String>();
		for (String record : EMP.split("\n")) {
			records.add(record);
		}
		return records;
	}

	protected void withInputFromDepartments(MapReduceDriver driver) {
		int i = 0;
		for (String line : getDepartmentRecords()) {
			driver.withInput(new LongWritable(i), new Text(line));
			i = nextOffset(i, line);
		}
	}

	protected List<String> getDepartmentRecords() {
		List<String> records = new ArrayList<String>();
		for (String record : DEPT.split("\n")) {
			records.add(record);
		}
		return records;
	}

	protected int nextOffset(int oldOffset, String line) {
		return oldOffset + line.length() + 1;
	}
}
