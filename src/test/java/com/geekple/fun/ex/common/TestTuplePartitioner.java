package com.geekple.fun.ex.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Daegeun Kim
 */
public class TestTuplePartitioner {
	private static int REAL = 200000;
	
	List<Key> real;
	List<Key> samples;
	
	@SuppressWarnings("unchecked")
	public void init() {
		real = new ArrayList<Key>();
		samples = new ArrayList<Key>();
		
		for (int i = 0; i < REAL; i++) {
			real.add(create(i));
		}
		Random r = new Random();
		r.setSeed(r.nextLong());
		
		for (int i = 0; i < 10000; i++) {
			samples.add(create(r.nextInt(REAL)));
		}
		Collections.sort(samples);
	}

	@Test
	public void testComputeSplitPoints() {
		float errorRate = 0.02f;
		for (int i = 1; i <= 20; i++) {
			errorRate *= 1.1;
			if (i % 4 > 0) {
				continue;
			}
			
			int falsecount = 0;
			float lastErrorRate = 0;
			for (int j = 0; j < 8; j++) {
				int[] counters = computeSplitPointsAndExecute(i);
				float actual = then(counters, errorRate);
				if (actual != 0 && errorRate < actual) {
					falsecount++;
					lastErrorRate = actual;
				}
			}
			Assert.assertTrue(String.format("파티션 오차범위가 10번 시도 중 3번이상 예상 범위 초과. 마지막 실패: " + lastErrorRate), falsecount < 3);
		}
	}

	private int[] computeSplitPointsAndExecute(int tasks) {
		init();
		TuplePartitioner partitioner = new TuplePartitioner();
		partitioner.splitPoints = TuplePartitioner.computeSplitPoints(tasks, samples.toArray(new Key[0]));
		int[] counters = new int[tasks];
		for (Key key : real) {
			counters[partitioner.getPartition(key, null, tasks)]++;
		}
		return counters;
	}

	/**
	 * 파티션 의미있게 나눠지는지 체크. 두번째 인자는 오차범위 (0 ~ 1)
	 * 
	 * @param counter
	 * @param errorRate 오차 (0 ~ 1)
	 * @return 
	 */
	public float then(int[] counters, float errorRate) {
		int divide = REAL / counters.length;
		System.out.println("error rate: " + (errorRate * 100) + "%, divide: " + divide);
		int total = 0;
		for (int i = 0; i < counters.length; i++) {
			int counter = counters[i];
			int error = Math.abs(divide - counter);
			System.out.println(String.format(
					"partition: %2d, counter: %6d, error: %5d (%1.3f%%), valid range: %s", 
					i, counter, error, error / (float) divide, (error  / (float) divide <= errorRate)
			));
			total += error;
		}
		return total / counters.length / (float) divide;
	}
	
	private Key create(long value) {
		Key key = new Key("$K", new WritableComparable[] { new LongWritable(value) });
		return key;
	}
}
