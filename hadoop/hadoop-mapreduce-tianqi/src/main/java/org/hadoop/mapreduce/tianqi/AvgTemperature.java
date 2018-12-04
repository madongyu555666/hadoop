package org.hadoop.mapreduce.tianqi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hadoop.mapreduce.tianqi.MinTemperature.MinTemperatureMapper;
import org.hadoop.mapreduce.tianqi.MinTemperature.MinTemperatureReducer;

public class AvgTemperature extends Configured implements Tool {

	// Mapper
	public static class AvgTemperatureMapper extends Mapper<LongWritable, Text, Text, Text> {
		private static final int MISSING = 9999;

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String year = line.substring(14, 18);
			int airTemperature;
			if (line.charAt(87) == '+') {
				airTemperature = Integer.parseInt(line.substring(88, 92));
			} else {
				airTemperature = Integer.parseInt(line.substring(26, 28));
			}
			String quality = line.substring(92, 93);
			if (airTemperature != MISSING && !quality.matches("[01459]")) {
				context.write(new Text(year), new Text(String.valueOf(airTemperature))); // System.out.println("Mapper输出<"
																							// +year + "," +
																							// airTemperature + ">"); }
																							// } }
			}
		}
	}

	// 对于平均值而言，各局部平均值的平均值将不再是整体的平均值了，所以不能直接用combiner。
	// 可以通过变通的办法使用combiner来计算平均值，即在combiner的键值对中不直接存储最后的平均值，
	// 而是存储所有值的和个数，最后在reducer输出时再用和除以个数得到平均值。
	// Combiner
	public static class AvgTemperatureCombiner extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			System.out.println("....AvgTemperatureCombiner.... ");
			double sumValue = 0;
			long numValue = 0;
			for (Text value : values) {
				sumValue += Double.parseDouble(value.toString());
				numValue++;
			}
			context.write(key, new Text(String.valueOf(sumValue) + ',' + String.valueOf(numValue)));
			System.out.println("Combiner输出键值对<" + key + "," + sumValue + "," + numValue + ">");
		}
	}

	// java.lang.NoSuchMethodException:
	// temperature.AvgTemperature$AvgTemperatureReducer.<init>()
	// Mapper和Reducer作为内部类必须是静态static
	// Reducer
	public static class AvgTemperatureReducer extends Reducer<Text, Text, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			System.out.println("....AvgTemperatureReducer.... ");
			double sumValue = 0;
			long numValue = 0;
			int avgValue = 0;
			for (Text value : values) {
				String[] valueAll = value.toString().split(",");
				sumValue += Double.parseDouble(valueAll[0]);
				numValue += Integer.parseInt(valueAll[1]);
			}
			avgValue = (int) (sumValue / numValue);
			context.write(key, new IntWritable(avgValue));
		}
	}
	
	
	
	

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		// 读取配置文件
		conf.set("fs.defaultFS", "hdfs://47.107.182.164:9000");
		Job job = Job.getInstance(conf, "Avg Temperature");
		job.setJarByClass(AvgTemperature.class);
		Path in = new Path(args[0]);
		// 输入路径
		Path out = new Path(args[1]);
		// 输出路径
		FileSystem hdfs = out.getFileSystem(conf);
		if (hdfs.isDirectory(out)) {
			// 如果输出路径存在就删除
			hdfs.delete(out, true);
		}
		FileInputFormat.setInputPaths(job, in);
		// 文件输入
		FileOutputFormat.setOutputPath(job, out);
		// 文件输出
		job.setMapperClass(AvgTemperatureMapper.class);
		job.setCombinerClass(AvgTemperatureCombiner.class);
		job.setReducerClass(AvgTemperatureReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;// 等待作业完成退出
	}

	public static void main(String[] args) {
		System.setProperty("HADOOP_USER_NAME", "root");
		try {
			// 程序参数：输入路径、输出路径
			String[] args0 = { "/user/root/temperature/input", "/user/root/temperature/output/" };
			// 本地运行：第三个参数可通过数组传入，程序中设置为args0
			// 集群运行：第三个参数可通过命令行传入，程序中设置为args
			// 这里设置为本地运行，参数为args0
			int res = ToolRunner.run(new Configuration(), new MaxTemperature(), args0);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
