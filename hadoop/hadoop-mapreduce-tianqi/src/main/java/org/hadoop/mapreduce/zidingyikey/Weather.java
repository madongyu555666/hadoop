package org.hadoop.mapreduce.zidingyikey;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 自定义key
 * 
 * @author madongyu-ds
 *
 */
public class Weather implements WritableComparable<Weather> {

	private int year;
	private int month;
	private double hot;

	public Weather() {
	}

	public Weather(int year, int month, double hot) {
		this.year = year;
		this.month = month;
		this.hot = hot;
	}

	@Override
	public String toString() {
		return "[year=" + year + ", month=" + month + "]";
	}

	 /**
     * 将对象转换为字节流并写入到输出流out中
     */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
        out.writeInt(month);
        out.writeDouble(hot);

	}

	 /**
     * 从输入流in中读取字节流反序列化为对象
     */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.year = in.readInt();
        this.month = in.readInt();
        this.hot = in.readDouble();
	}
	
	
	
	 // 判断对象是否是同一个对象，当该对象作为输出的key
	@Override
	public int compareTo(Weather t) {
		 int r1 = Integer.compare(this.year, t.getYear());
		 if (r1 == 0) {
			 //如果年份相同，则判断月份 
			 int r2 = Integer.compare(this.month, t.getMonth()); 
			   if (r2 == 0) {
				   return Double.compare(this.hot, t.getHot()); 
			   } else { return r2; }
			
		 }else {
			 return r1;
		}
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public double getHot() {
		return hot;
	}

	public void setHot(double hot) {
		this.hot = hot;
	}

     


}
