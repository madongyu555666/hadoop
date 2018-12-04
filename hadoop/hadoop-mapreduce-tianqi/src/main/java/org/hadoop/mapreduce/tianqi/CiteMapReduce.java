package org.hadoop.mapreduce.tianqi;

import java.io.IOException;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * 专利
 * @author 
 *
 */
public class CiteMapReduce extends Configured implements Tool {

	// 静态Mapper类
	public static class MapTemplate extends Mapper<Text, Text, Text, Text> {

		/**
		 * Text key：每行文件的 key 值（即引用的专利）。 Text value：每行文件的 value 值（即被引用的专利）。
		 * map方法把字符串解析成Key-Value的形式，发给 Reduce 端来统计。
		 */
		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			// 根据业务需求(value被key引用)，将key和value调换输出
			context.write(value, key);
		}
	}

	// 静态Reducer类
	public static class ReduceTemplate extends Reducer<Text, Text, Text, Text> {

		/**
		 * 获取map方法的key-value结果,相同Key发送到同一个reduce里处理, 然后迭代values集合，把Value相加，结果写到 HDFS
		 * 系统里面
		 */
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String csv = "";
			// 将引入相同专利编号拼接输出
			for (Text val : values) {
				if (csv.length() > 0)
					csv += ",";
				// 添加分隔符
				csv += val.toString();
			}
			context.write(key, new Text(csv));
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		// 读取配置文件
		Configuration conf = getConf();
		// 设置参数
		conf.set("fs.defaultFS", "hdfs://47.107.182.164:9000");

		// 自定义key value 之间的分隔符（默认为tab）
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
		// Job表示一个MapReduce任务,构造器第二个参数为Job的名称。
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(MapReduceTemplate.class);// 主类
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
		job.setMapperClass(MapTemplate.class);
		// 设置自定义Mapper
		job.setReducerClass(ReduceTemplate.class);
		// 设置自定义Reducer
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		// 文件输入格式
		job.setOutputFormatClass(TextOutputFormat.class);
		// 文件输出格式
		job.setOutputKeyClass(Text.class);
		// 设置作业输出值 Key 的类
		job.setOutputValueClass(Text.class);
		// 设置作业输出值 Value 的类
		return job.waitForCompletion(true) ? 0 : 1;// 等待作业完成退出
	}

	/**
	 * @param args输入文件、输出路径，可在Eclipse的Run Configurations中配如：
	 */
	public static void main(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME", "root");
		try {
			// 程序参数：输入路径、输出路径
			String[] args0 = { "/user/root/cite/input/cite75_99.txt", "/user/root/cite/output/" };
			// 本地运行：第三个参数可通过数组传入，程序中设置为args0
			// 集群运行：第三个参数可通过命令行传入，程序中设置为args
			// 这里设置为本地运行，参数为args0
			int res = ToolRunner.run(new Configuration(), new CiteMapReduce(), args0);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
