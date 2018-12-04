package org.hadoop.hdfs.api;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 上传文件
 * @author
 *
 */
public class PutFile {

	public static void main(String[] args) throws IOException, InterruptedException {
		/*
		 * String local="D:\\word.txt"; String
		 * dest="hdfs://47.107.182.164:9000/input/word2.txt"; Configuration cfg = new
		 * Configuration(); FileSystem fs = FileSystem.get(URI.create(dest), cfg,
		 * "root"); fs.copyToLocalFile(new Path(local), new Path(dest)); fs.close();
		 */
		String local = "D:\\word.txt";
		String dest = "hdfs://47.107.182.164:9000/tmp/word2.txt";
		Configuration cfg = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dest), cfg, "root");
		fs.copyFromLocalFile(new Path(local), new Path(dest));
		fs.close();
	}

}
