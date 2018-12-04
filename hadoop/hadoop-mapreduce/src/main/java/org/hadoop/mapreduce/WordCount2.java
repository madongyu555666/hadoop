package org.hadoop.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.hadoop.mapreduce.WordCount.IntSumReducer;
import org.hadoop.mapreduce.WordCount.TokenizerMapper;

public class WordCount2 {

	
	//4种形式的参数，分别用来指定map的输入key值类型、输入value值类型、输出key值类型和输出value值类型
		public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
			private final static IntWritable one = new IntWritable(1);
			private Text word = new Text(); 
			//map方法中value值存储的是文本文件中的一行（以回车符为行结束标记），而key值为该行的首字母相对于文本文件的首地址的偏移量
			public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				StringTokenizer itr = new StringTokenizer(value.toString());
				//StringTokenizer类将每一行拆分成为一个个的单词，并将<word,1>作为map方法的结果输出 
				while (itr.hasMoreTokens()) {
					word.set(itr.nextToken()); 
					context.write(word, one); 
				} 
			 }
		}
		
		
		
		 public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
			 private IntWritable result = new IntWritable(); 
			 //Map过程输出<key,values>中key为单个单词，而values是对应单词的计数值所组成的列表，Map的输出就是Reduce的输入，
			 //所以reduce方法只要遍历values并求和，即可得到某个单词的总次数。
			 public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException { 
				 int sum = 0; 
				 for (IntWritable val : values) { 
					 sum += val.get(); 
				  } 
				 result.set(sum);
				 context.write(key, result); 
				 
			 }
		 }
		 
		 
		//执行MapReduce任务 
		 public static void main(String[] args) throws Exception {
			 Configuration conf = new Configuration();
			 Job job = Job.getInstance(conf, "wordCount"); 
			 job.setJarByClass(WordCount.class); 
			 job.setMapperClass(TokenizerMapper.class);
			 job.setCombinerClass(IntSumReducer.class);
			 job.setReducerClass(IntSumReducer.class);
			 job.setOutputKeyClass(Text.class);
			 job.setOutputValueClass(IntWritable.class);
			 //开启combine过程
			   job.setCombinerClass(IntSumReducer.class);
			   
			 //命令行输入的第一个参数是输入路径，第二个参数是输出路径 
			 FileInputFormat.addInputPath(job, new Path(args[0]));
			 FileOutputFormat.setOutputPath(job, new Path(args[1]));
			 System.exit(job.waitForCompletion(true) ? 0 : 1);
			 
		 }
		

}
