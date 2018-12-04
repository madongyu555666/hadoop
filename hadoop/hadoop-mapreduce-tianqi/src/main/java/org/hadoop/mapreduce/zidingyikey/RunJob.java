package org.hadoop.mapreduce.zidingyikey;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 测试方法
 * 
 * @author madongyu-ds
 *
 */
public class RunJob {


	public static class WeatherMapper extends Mapper<LongWritable, Text, Weather, DoubleWritable> {
		private static final String MISSING = "9999.9";

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String val = value.toString();
			int year = 0;
			int month = 0;
			double hot = 0.0;
			Weather w = null;
			try {
				year = Integer.parseInt(val.substring(14, 18));
				month = Integer.parseInt(val.substring(18, 20));
				String hotStr = val.substring(102, 108);
				if (!MISSING.equals(hotStr)) {
					hot = Double.parseDouble(hotStr);
					w = new Weather(year, month, hot);
					context.write(w, new DoubleWritable(hot));
				}
			} catch (Exception e) {
				System.out.println(e);
			}
		}
	}

	public static class WeatherReducer extends Reducer<Weather, DoubleWritable, Text, DoubleWritable> {
		protected void reduce(Weather key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			double maxValue = 0.0;
			for (DoubleWritable value : values) {
				maxValue = Math.max(maxValue, value.get());
			}
			context.write(new Text(key.toString()), new DoubleWritable(maxValue));
		}
	}
	
	
	
	public static void main(String[] args) {
		// 设置环境变量HADOOP_USER_NAME，其值是root
		System.setProperty("HADOOP_USER_NAME", "root");
		// Configuration类包含了Hadoop的配置
		Configuration config = new Configuration();
		// 设置fs.defaultFS
		config.set("fs.defaultFS", "hdfs://47.107.182.164:9000");
		// 设置yarn.resourcemanager节点
		config.set("yarn.resourcemanager.hostname", "node2");
		try {
			FileSystem fs = FileSystem.get(config);
			Job job = Job.getInstance(config);
			job.setJarByClass(RunJob.class);
			job.setJobName("weather");
			job.setMapperClass(WeatherMapper.class);
			job.setReducerClass(WeatherReducer.class);
			job.setMapOutputKeyClass(Weather.class);
			job.setMapOutputValueClass(DoubleWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
			job.setPartitionerClass(MyPartitioner.class);
			job.setSortComparatorClass(MyComparator.class);
			// 只有两年的数据，所以ReduceTask设置1
			job.setNumReduceTasks(1);
			FileInputFormat.addInputPath(job, new Path("/user/root/temperature/input/temperature.txt"));
			Path outpath = new Path("/user/root/temperature2/output/");
			if (fs.exists(outpath)) {
				fs.delete(outpath, true);
			}
			FileOutputFormat.setOutputPath(job, outpath);
			System.out.println(job.waitForCompletion(true));

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
