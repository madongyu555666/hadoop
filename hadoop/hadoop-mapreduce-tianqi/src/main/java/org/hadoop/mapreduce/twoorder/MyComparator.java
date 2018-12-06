package org.hadoop.mapreduce.twoorder;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


/**
 * 自定义比较区
 * 
 * @author madongyu-ds
 *
 */
public class MyComparator extends WritableComparator{

	
	 protected MyComparator(){
	        super(Text.class,true);
	 }
	 
	@Override
	public int compare(WritableComparable k1, WritableComparable k2) {
		 String k11=k1.toString().trim();
		 String replaceAll = k11.replaceAll(" ", ",");
		 String[] a1=replaceAll.toString().split(",");
		
		 String k22=k2.toString().trim();
		 String replaceAll2 = k22.replaceAll(" ", ",");
	     String[] a2=replaceAll2.toString().split(",");
	     //如果种类字段相同，则比较价格字段
	     if(a1[8].equals(a2[8])){
	    	//如果价格也相同，如果返回0，则认为是相同的书；所以需要进一步比较书名
	            if(a1[4].equals(a2[4])){
	            	 return a1[10].compareTo(a2[10]);
	            }else{
	                return a1[4].compareTo(a2[4]);
	            }
	     }else{
	            return a1[8].compareTo(a2[8]);
	     }
	}
}
