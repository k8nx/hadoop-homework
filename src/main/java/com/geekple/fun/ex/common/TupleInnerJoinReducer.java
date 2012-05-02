package com.geekple.fun.ex.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Daegeun Kim
 */
public class TupleInnerJoinReducer extends Reducer<Key, TupleEx, NullWritable, TupleEx> {
	private Map<String, String[]> projections;
	private List<TupleEx> merged;
	private Map<String, List<TupleEx>> cached;

	protected void setup(Reducer<Key, TupleEx, NullWritable, TupleEx>.Context context) throws IOException,
			InterruptedException {
		String projectionExpression = context.getConfiguration().get(TupleInputFormat.PROJECTION_EXPRESSION, "").trim();
		projections = ExpressionUtils.parseProjectionExpression(projectionExpression);
		TupleInputFormat.initTableFieldSchema(context.getConfiguration());
	}

	protected void reduce(Key key, Iterable<TupleEx> values, Reducer<Key, TupleEx, NullWritable, TupleEx>.Context context) throws IOException, InterruptedException {
		merged = new ArrayList<TupleEx>();
		cached = new LinkedHashMap<String, List<TupleEx>>();
		
		Iterator<TupleEx> it = values.iterator();
		TupleEx current;
		while (it.hasNext()) {
			current = it.next();
			if (cached.size() != projections.size()) {
				if (!cached.containsKey(current.alias)) {
					cached.put(current.alias, new ArrayList<TupleEx>());
				}
				if (cached.size() != projections.size()) {
					cached.get(current.alias).add(current.export(projections.get(current.alias)));
				} else {
					merge();
					write(context, key, current);
				}
			} else {
				write(context, key, current);
			}
		}
	}
	
	private void merge() {
		boolean first = true;
		for (String key : cached.keySet()) {
			if (first) {
				for (TupleEx tuple : cached.get(key)) {
					merged.add(TupleEx.create("$P").merge(null, tuple));
				}
			} else {
				List<TupleEx> temp = new ArrayList<TupleEx>();
				for (TupleEx tuple : merged) {
					for (TupleEx newtuple : cached.get(key)) {
						temp.add(tuple.merge(null, newtuple));
					}
				}
				if (temp.size() > 0) {
					merged = temp;
				}
			}
			first = false;
		}
	}

	private void write(Reducer<Key, TupleEx, NullWritable, TupleEx>.Context context, Key key, TupleEx current) throws IOException, InterruptedException {
		TupleEx exported = current.export(projections.get(current.alias));
		for (TupleEx tuple : merged) {
			TupleEx m = tuple.merge(tuple.alias, exported);
			doWrite(context, m);
		}
	}
	
	/**
	 * Tuple 을 수정하고 싶은 경우나 추가적인 작업이 필요한 경우 이 메서드를 재구현하면 됩니다.
	 * @param context
	 * @param tuple
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected void doWrite(Reducer<Key, TupleEx, NullWritable, TupleEx>.Context context, TupleEx tuple) throws IOException, InterruptedException {
		context.write(NullWritable.get(), tuple);
	}
}