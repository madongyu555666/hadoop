package org.hadoop.mapreduce.zidingyikey;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义比较区
 * 
 * @author madongyu-ds
 *
 */
public class MyComparator extends WritableComparator {

	protected MyComparator() {
	     super(Weather.class, true);
	 }

	@Override public int compare(WritableComparable k1, WritableComparable k2) {
		Weather key1=(Weather)k1; 
		Weather key2=(Weather)k2; 
		int r1 = Integer.compare(key1.getYear(), key2.getYear());
		if (r1 == 0) { 
			//如果年份相同，则判断月份
			return Integer.compare(key1.getMonth(), key2.getMonth()); 
		 } else { 
			 return r1; 
		 } 
		
	}
	
}
