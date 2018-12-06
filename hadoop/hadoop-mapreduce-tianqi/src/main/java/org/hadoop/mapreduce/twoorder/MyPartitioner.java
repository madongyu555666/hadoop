package org.hadoop.mapreduce.twoorder;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;


/**
 * map的输出
 * @author madongyu-ds
 *
 */
public class MyPartitioner extends HashPartitioner<Text, NullWritable> {
	
	/**
	 * 执行的时间越短越好
	 */
	@Override
	public int getPartition(Text key, NullWritable value, int numReduceTasks) {
		// TODO Auto-generated method stub
		return (key.toString().split(",")[2].hashCode() & Integer.MAX_VALUE)%numReduceTasks;
	}
}
