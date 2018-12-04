package org.hadoop.mapreduce.zidingyikey;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class MyPartitioner extends HashPartitioner<Weather, DoubleWritable>{

	 // 执行时间越短越好 
	public int getPartition(Weather key, DoubleWritable value, int numReduceTasks) { 
		// 根据年份分区 
		return key.getYear() % numReduceTasks; 
	}

}
