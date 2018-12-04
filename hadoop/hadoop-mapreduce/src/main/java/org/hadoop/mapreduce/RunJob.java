package org.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RunJob {

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
			job.setJobName("wc");
			// 设置Mapper类
			job.setMapperClass(WordCountMapper.class);
			// 设置Reduce类
			job.setReducerClass(WordCountReducer.class);
			// 设置reduce方法输出key的类型
			job.setOutputKeyClass(Text.class);
			// 设置reduce方法输出value的类型
			job.setOutputValueClass(IntWritable.class);
			// 指定输入路径
			FileInputFormat.addInputPath(job, new Path("/user/root/input/input/"));
			// 指定输出路径（会自动创建）
			Path outpath = new Path("/user/root/output/");
			// 输出路径是MapReduce自动创建的，如果存在则需要先删除
			if (fs.exists(outpath)) {
				fs.delete(outpath, true);
			}
			FileOutputFormat.setOutputPath(job, outpath);
			// 提交任务，等待执行完成
			boolean f = job.waitForCompletion(true);
			if (f) {
				System.out.println("job任务执行成功");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
