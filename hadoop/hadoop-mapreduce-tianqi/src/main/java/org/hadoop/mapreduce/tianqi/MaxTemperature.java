package org.hadoop.mapreduce.tianqi;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 测试完成
 * 采集的气象数据格式的问题
 * 求最高温度
 * 
 * 
 *
 */
public class MaxTemperature extends Configured implements Tool {

	// mappper
	public static class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		// 9999代码表示缺失
		private static final int MISSING = 9999;

		@Override
		protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
			
				String line = value.toString();
				int length = line.length();
				String year = line.substring(14, 18);
				int airTemperature;
				if (line.charAt(87) == '+') {
					airTemperature = Integer.parseInt(line.substring(88, 92));
				} else {
					
					String substring = line.substring(26, 28);
					airTemperature = Integer.parseInt(line.substring(26, 28));
				}
				String quality = line.substring(92, 93);
				if (airTemperature != MISSING && quality.matches("[01459]")) {
					context.write(new Text(year), new IntWritable(airTemperature));
				}
		}
	}

	// reduce
	public static class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)  throws IOException, InterruptedException{
			
			int maxValue = Integer.MIN_VALUE;
	       for(IntWritable value:values){
				maxValue = Math.max(maxValue, value.get());
			}
	       context.write(key,new IntWritable(maxValue));
	
		}
		
	
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf(); 
		//读取配置文件 
		conf.set("fs.defaultFS", "hdfs://47.107.182.164:9000");
		Job job = Job.getInstance(conf, "MaxTemperature"); 
		job.setJarByClass(MaxTemperature.class); 
		Path in = new Path(args[0]);
		//输入路径 
		Path out = new Path(args[1]);
		//输出路径 
		FileSystem hdfs = out.getFileSystem(conf);
		if (hdfs.isDirectory(out)) {
			//如果输出路径存在就删除
			hdfs.delete(out, true); 
			} 
		FileInputFormat.setInputPaths(job, in);
		//文件输入 
		FileOutputFormat.setOutputPath(job, out);
		//文件输出 
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		job.setOutputKeyClass(Text.class); 
		job.setOutputValueClass(IntWritable.class); 
		return job.waitForCompletion(true)?0:1;//等待作业完成退出 
	  }
	
	public static void main(String[] args) {
		 System.setProperty("HADOOP_USER_NAME", "root");
		 try {
			 //程序参数：输入路径、输出路径 
			 String[] args0 ={"/user/root/temperature/input","/user/root/temperature/output/"}; 
			 //本地运行：第三个参数可通过数组传入，程序中设置为args0  
			 //集群运行：第三个参数可通过命令行传入，程序中设置为args 
			 //这里设置为本地运行，参数为args0 
			 int res = ToolRunner.run(new Configuration(), new MaxTemperature(), args0);
			 System.out.println(res);//计算完成了，打印0
			 System.exit(res);
		 }catch (Exception e) {
			 e.printStackTrace();
		}
	}	
}


