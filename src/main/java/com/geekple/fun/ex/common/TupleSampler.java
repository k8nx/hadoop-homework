package com.geekple.fun.ex.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler.Sampler;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

/** 
 * @author Daegeun Kim
 */
public class TupleSampler {
	/**
     * 빌트인 되어있는 RandomSampler 와 비슷함.
	 * @author Daegeun Kim
	 */
	public static class RandomSampler  implements Sampler<Key, TupleEx> {
		private double freq;
		private int numSamples;
		private int maxSplitsSampled;
		
		/**
		 * @param freq 확률
		 * @param numSamples 최대 샘플링할 튜플 개수
		 * @param maxSplitsSampled 스플릿 최대 개수
		 */
		public RandomSampler(double freq, int numSamples, int maxSplitsSampled) {
			this.freq = freq;
			this.numSamples = numSamples;
			this.maxSplitsSampled = maxSplitsSampled;
		}

		@Override
		public Key[] getSample(InputFormat<Key, TupleEx> inf, Job job) throws IOException, InterruptedException {
			List<InputSplit> splits = inf.getSplits(job);
			int splitsSampled = Math.max(maxSplitsSampled, splits.size());
			List<Key> keys = new ArrayList<Key>(numSamples);
			
			Random r = new Random();
			r.setSeed(r.nextLong());

			for (int i = 0; i < splits.size(); i++) {
				InputSplit t = splits.get(i);
				int j = r.nextInt(splits.size());
				splits.set(i, splits.get(j));
				splits.set(j, t);
			}
			
			for (int i = 0; i < splitsSampled || (i < splits.size() && keys.size() < numSamples) ; i++) {
				if (i >= splits.size()) {
					break;
				}
				InputSplit split = splits.get(i);
				RecordReader<Key, TupleEx> reader = inf.createRecordReader(null, null);
				reader.initialize(split, new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID()));
				reader.getProgress();
				while (reader.nextKeyValue()) {
					if (r.nextDouble() < freq) {
						if (keys.size() < numSamples) {
							keys.add(reader.getCurrentKey());
						} else {
							// 최대 샘플링 개수 넘었을 경우 임의의 index 의 값 교체 후 확률 축소
							keys.set(r.nextInt(keys.size() - 1), reader.getCurrentKey());
							freq *= (numSamples - 1) / (float) numSamples;
						}
					}
				}
				reader.close();
			}
			return keys.toArray(new Key[0]);
		}
	}
}
