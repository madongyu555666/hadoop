package org.hadoop.mapreduce.tianqi;

import java.io.IOException;
import java.util.StringTokenizer; // 分隔字符串 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
// 相当于int类型 
import org.apache.hadoop.io.LongWritable;
// 相当于long类型 
import org.apache.hadoop.io.Text;
// 相当于String类型 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * mapReduce模板
 * 
 * @author 
 *
 */
public class MapReduceTemplate extends Configured implements Tool {

	
	//静态Mapper类
	public static class MapTemplate extends Mapper<LongWritable, Text, Text, IntWritable> {
		@Override 
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
			// 将输入数据解析成Key/Value对 
			// TODO: map()方法实现
			} 
		} 
	
	
	//静态Reducer类
	public static class ReduceTemplate extends Reducer<Text, IntWritable, Text, IntWritable> { 
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException { 
			// TODO: reduce() 方法实现 
			} 
		}
	
	
	@Override
	public int run(String[] args) throws Exception {
		 //读取配置文件
        Configuration conf = getConf(); 
        //设置参数
        conf.set("fs.defaultFS", "hdfs://47.107.182.164:9000");
		
        //自定义key value 之间的分隔符（默认为tab） 
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ","); 
        // Job表示一个MapReduce任务,构造器第二个参数为Job的名称。 
        Job job = Job.getInstance(conf, "MapReduceTemplate"); 
        job.setJarByClass(MapReduceTemplate.class);//主类
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
        job.setMapperClass(MapTemplate.class);
        //设置自定义Mapper
        job.setReducerClass(ReduceTemplate.class); 
        //设置自定义Reducer 
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        //文件输入格式 
        job.setOutputFormatClass(TextOutputFormat.class);
        //文件输出格式 
        job.setOutputKeyClass(Text.class);
        //设置作业输出值 Key 的类 
        job.setOutputValueClass(Text.class);
        //设置作业输出值 Value 的类 
        return job.waitForCompletion(true)?0:1;//等待作业完成退出
	} 
	
	  //主方法，程序入口，调用ToolRunner.run( )
	 public static void main(String[] args) throws Exception { 
		 int exitCode = ToolRunner.run(new MapReduceTemplate(), args);
		 System.exit(exitCode); 
	 } 

}
