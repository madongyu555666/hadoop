package org.hadoop.mapreduce.twoorder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
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
					job.setJobName("Sort2");
					job.setMapperClass(Sort2Mapper.class);
					job.setReducerClass(Sort2Reducer.class);
					 //设置map方法输出类型
		            job.setMapOutputKeyClass(Text.class);
		            job.setMapOutputValueClass(NullWritable.class);
		            
		         // 设置reduce方法输出key和value的类型
		            job.setOutputKeyClass(NullWritable.class);
		            job.setOutputValueClass(Text.class);
					
		          //设置自定义工具类
		            job.setPartitionerClass(MyPartitioner.class);
		            job.setSortComparatorClass(MyComparator.class);
		          //job.setGroupingComparatorClass(MyGroup.class);
		            //设置Reduce Task数
		            //job.setNumReduceTasks(3);
					
					
		            // 指定输入输出路径 
					FileInputFormat.addInputPath(job, new Path("/user/root/input/books.txt"));
					Path outpath = new Path("/user/root/output/");
					if (fs.exists(outpath)) {
						fs.delete(outpath, true);
					}
					FileOutputFormat.setOutputPath(job, outpath);
					System.out.println(job.waitForCompletion(true));
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
