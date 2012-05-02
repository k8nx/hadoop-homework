package com.geekple.fun.ex.common;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler.Sampler;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * @author Daegeun Kim
 */
@SuppressWarnings("deprecation")
public class TuplePartitioner extends Partitioner<Key, TupleEx> implements Configurable {
	public static final String USE_GROUP_COMPARATOR = "tuple.partition.use_group_comparator";
	public static final String PARTITION_FILE = "tuple.partition.file";
	private static final Log LOG = LogFactory.getLog(TuplePartitioner.class);

	List<Key> splitPoints = new ArrayList<Key>();
	private Configuration conf;

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		String partitionFile = TuplePartitioner.getPartitionFile(conf);
		Path p = new Path(partitionFile);
		FileSystem fs;
		try {
			fs = p.getFileSystem(conf);
			for (Key key : readPartitions(fs, p, Key.class, conf)) {
				splitPoints.add(key);
			}
		} catch (IOException e) {
		}
	}

	@Override
	public Configuration getConf() {
		return conf;
	}
	
	@Override
	public int getPartition(Key key, TupleEx value, int numPartitions) {
		for (int i = 0; i < splitPoints.size(); i++) {
			if (key.compareTo(splitPoints.get(i)) == -1) {
				return i;
			}
		}
		return numPartitions - 1;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void writePartitionFile(Job job, Sampler<Key, TupleEx> sampler) throws IOException, ClassNotFoundException, InterruptedException {
		LOG.info("Start Sampling.");
	    Configuration conf = job.getConfiguration();
	    final InputFormat inf = ReflectionUtils.newInstance(job.getInputFormatClass(), conf);
	    int numPartitions = job.getNumReduceTasks();
	    Key[] samples = sampler.getSample(inf, job);
	    LOG.info("Using " + samples.length + " samples");
	    RawComparator<Key> comparator = isUseGroupComparator(conf) ?
	    		(RawComparator<Key>) job.getGroupingComparator() : (RawComparator<Key>) job.getSortComparator();
	    Arrays.sort(samples, comparator);
	    Path dst = new Path(getPartitionFile(conf));
	    FileSystem fs = dst.getFileSystem(conf);
	    if (fs.exists(dst)) {
	      fs.delete(dst, false);
	    }
	    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, dst, job.getMapOutputKeyClass(), NullWritable.class);
	    NullWritable nullValue = NullWritable.get();
	    for (Key key : computeSplitPoints(numPartitions, samples)) {
	    	writer.append(key, nullValue);
	    }
	    writer.close();
	}
	
	static List<Key> computeSplitPoints(int numPartitions, Key[] samples) {
		List<Key> splitPoints = new ArrayList<Key>();
		int numSamples = samples.length;
		float divide = numSamples / (float) numPartitions;
		for (float current = divide; current < numSamples; current += divide) {
			int i = (int) current;
			splitPoints.add(samples[i]);
		}
		return splitPoints;
	}

	private Key[] readPartitions(FileSystem fs, Path p, Class<Key> keyClass, Configuration conf) throws IOException {
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, p, conf);
		ArrayList<Key> parts = new ArrayList<Key>();
		Key key = ReflectionUtils.newInstance(keyClass, conf);
		NullWritable value = NullWritable.get();
		while (reader.next(key, value)) {
			parts.add(key);
			key = ReflectionUtils.newInstance(keyClass, conf);
		}
		reader.close();
		return parts.toArray((Key[]) Array.newInstance(keyClass, parts.size()));
	}

	public static void setPartitionFile(Configuration conf, Path path) {
		conf.set(PARTITION_FILE, path.toString());
	}

	public static String getPartitionFile(Configuration conf) {
		return conf.get(PARTITION_FILE);
	}

	public static void useGroupComparator(Configuration conf, boolean use) {
		conf.setBoolean(USE_GROUP_COMPARATOR, use);
	}
	
	public static boolean isUseGroupComparator(Configuration conf) {
		return conf.getBoolean(USE_GROUP_COMPARATOR, true);
	}
}
