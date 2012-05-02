package com.geekple.fun.ex.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.StringUtils;

/**
 * @author Daegeun Kim
 */
@SuppressWarnings("deprecation")
public class TupleInputFormat extends FileInputFormat<Key, TupleEx> {
	public static final String JOIN_EXPRESSION = "tuple.expression.join";
	public static final String PROJECTION_EXPRESSION = "tuple.expression.projection";
	public static final String TUPLE_INPUT_ALIASES = "tuple.input.alias";
	public static final String TUPLE_INPUT_FILES = "tuple.input.file";
	public static final String TUPLE_INPUT_DEFINITIONS = "tuple.input.definition";

	public static void initTableFieldSchema(Configuration conf) {
		String aliases = conf.get(TUPLE_INPUT_ALIASES, null);
		if (aliases != null) {
			for (String alias : aliases.split(",")) {
				TupleEx.registerFieldSchema(alias, conf.get(TUPLE_INPUT_DEFINITIONS + "." + alias));
			}
		}
	}

	public static void addTable(Job job, String alias, Path path, String definition) {
		Configuration conf = job.getConfiguration();
		try {
			conf.set(TUPLE_INPUT_FILES + "." + alias, StringUtils.escapeString(path.getFileSystem(conf).makeQualified(path).toString()));
			addFieldDefinition(job.getConfiguration(), alias, definition);
			addAlias(job.getConfiguration(), alias);
		} catch (IOException e) {
		}
	}
	
	private static void addFieldDefinition(Configuration conf, String alias, String definition) {
		conf.set(TUPLE_INPUT_DEFINITIONS + "." + alias, definition);
		TupleEx.registerFieldSchema(alias, definition);
	}

	private static void addAlias(Configuration conf, String alias) {
		String aliases = conf.get(TUPLE_INPUT_ALIASES, null);
		conf.set(TUPLE_INPUT_ALIASES, (aliases == null ? "" : aliases + ",") + alias);
	}

	public static void setJoinExpression(Job job, String expression) {
		Map<String, String[]> conditions = ExpressionUtils.parseJoinExpression(expression);
		StringBuilder sb = new StringBuilder();
		for (String alias : conditions.keySet()) {
			boolean first = true;
			for (String field : conditions.get(alias)) {
				sb.append(first ? "" : ",").append(field).append(":").append(TupleEx.getFieldSchema(alias).getField(field).type.name().toLowerCase());
				first = false;
			}
			break;
		}
		addAlias(job.getConfiguration(), "$K");
		addFieldDefinition(job.getConfiguration(), "$K", sb.toString());
		job.getConfiguration().set(JOIN_EXPRESSION, expression);
	}

	public static void setProjections(Job job, String expression) {
		List<String[]> projections = ExpressionUtils.getProjectionFieldsByExpression(expression);
		addAlias(job.getConfiguration(), "$P");
		StringBuilder definition = new StringBuilder();
		boolean first = true;
		for (String[] pair : projections) {
			definition.append(first ? "" : ",").append(pair[1]).append(":").append(TupleEx.getFieldSchema(pair[0]).getField(pair[1]).type.name().toLowerCase());
			first = false;
		}
		if (definition.length() == 0) {
			throw new TupleEx.IllegalFieldDefinitionException("invalid projections");
		}
		addFieldDefinition(job.getConfiguration(), "$P", definition.toString());
		job.getConfiguration().set(TupleInputFormat.PROJECTION_EXPRESSION, expression);
	}
	
	@Override
	protected List<FileStatus> listStatus(JobContext job) throws IOException {
		List<FileStatus> files = new ArrayList<FileStatus>();
		Configuration conf = job.getConfiguration();
		String aliases = conf.get(TUPLE_INPUT_ALIASES);
		for (String alias : StringUtils.split(aliases)) {
			if (conf.get(TUPLE_INPUT_FILES + "." + alias) == null) {
				continue;
			}
			Path path = new Path(conf.get(TUPLE_INPUT_FILES + "." + alias));
			for (FileStatus status : path.getFileSystem(conf).listStatus(path)) {
				if (status.isFile()) {
					files.add(status);
				}
			}
		}
		return files;
	}
	
	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		List<InputSplit> splits = super.getSplits(job);
		for (InputSplit inputSplit : splits) {
			if (inputSplit instanceof TupleSplit) {
				TupleSplit split = (TupleSplit) inputSplit;
				split.setAlias(getAlias(job, split.getPath()));
			}
		}
		return splits;
	}
	
	@Override
	protected FileSplit makeSplit(Path file, long start, long length, String[] hosts) {
		return new TupleSplit("", file, start, length, hosts);
	}
	
	private String getAlias(JobContext job, Path file) {
		Configuration conf = job.getConfiguration();
		String aliases = conf.get(TUPLE_INPUT_ALIASES);
		for (String alias : StringUtils.split(aliases)) {
			Path path = new Path(conf.get(TUPLE_INPUT_FILES + "." + alias));
			if (file.toString().startsWith(path.toString())) {
				return alias;
			}
		}
		return "";
	}

	@Override
	public RecordReader<Key, TupleEx> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new TupleRecordReader();
	}

	/**
	 * 현재 압축포맷 지원하지 않음.
	 * @author Daegeun Kim
	 */
	public static class TupleRecordReader extends RecordReader<Key, TupleEx> {
		private FSDataInputStream fileIn;
		private LineReader in;
		private int maxLineLength;
		private long start;
		private long end;
		private long pos;
		
		private Key key;
		private Text line;
		private String alias;
		private TupleEx value;

		private Map<String, String[]> conditions;
		
		@Override
		public void initialize(InputSplit tsplit, TaskAttemptContext context) throws IOException, InterruptedException {
			TupleSplit split = (TupleSplit) tsplit;
			Configuration conf = context.getConfiguration();
			
			String joinExpression = context.getConfiguration().get(TupleInputFormat.JOIN_EXPRESSION, "").trim();
			conditions = ExpressionUtils.parseJoinExpression(joinExpression);
			TupleInputFormat.initTableFieldSchema(conf);
			
			alias = split.getAlias();
			
			fileIn = split.getPath().getFileSystem(conf).open(split.getPath());
		    maxLineLength = Integer.MAX_VALUE;
			start = split.getStart();
			end = start + split.getLength();
			fileIn.seek(start);
			in = new LineReader(fileIn, conf);
			if (start != 0) {
				start += in.readLine(new Text(), 0, maxBytesToConsume(start));
			}
			this.pos = start;
		}

		private int maxBytesToConsume(long start) {
			return (int) Math.min(Integer.MAX_VALUE, end - start);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (value == null) {
				value = new TupleEx();
				value.alias = alias;
				line = new Text();
			}
			int newSize = 0;
			while (getFilePosition() <= end) {
				newSize = in.readLine(line, maxLineLength, Math.max(maxBytesToConsume(pos), maxLineLength));
				if (newSize == 0) {
					break;
				}
				pos += newSize;
				if (newSize < maxLineLength) {
					break;
				}
			}
			if (newSize == 0) {
				key = null;
				line = null;
				value = null;
				return false;
			} else {
				TupleEx.create(value, line);
				key = value.exportKey(conditions.get(alias));
			}
			return true;
		}

		@Override
		public Key getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public TupleEx getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		public float getProgress() throws IOException {
			if (start == end) {
				return 0.0f;
			} else {
				return Math.min(1.0f, (getFilePosition() - start) / (float) (end - start));
			}
		}

		private long getFilePosition() throws IOException {
			return pos;
		}

		public synchronized void close() throws IOException {
			try {
				if (in != null) {
					in.close();
				}
			} finally {
			}
		}
	}
}
