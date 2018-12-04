package org.hadoop.hdfs.api;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 创建目录
 * 
 * @author 
 *
 */
public class CreateDir {

	public static void main(String[] args) throws IOException, InterruptedException {
		String url = "hdfs://47.107.182.164:9000/tmp/";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(url), conf, "root");
		boolean b = fs.mkdirs(new Path(url));
		System.out.println(b);
		fs.close();

	}
}
