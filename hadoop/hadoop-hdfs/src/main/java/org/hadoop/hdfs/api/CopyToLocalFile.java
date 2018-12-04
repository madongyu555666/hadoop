package org.hadoop.hdfs.api;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 下载HDFS目录
 * 
 * @author 
 *
 */
public class CopyToLocalFile {

	public static void main(String[] args) throws IOException {
		String hdfsPath = "hdfs://47.107.182.164:9000/input";
		String localPath = "D:\\input";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		Path hdfs_path = new Path(hdfsPath);
		Path local_path = new Path(localPath);
		fs.copyToLocalFile(false,hdfs_path, local_path,true);
		fs.close();
	}

}
