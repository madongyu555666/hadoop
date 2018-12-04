package org.hadoop.hdfs.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.commons.lang.StringUtils;

/**
 * 封装的工具类
 * 
 * @author madongyu
 *
 */
public class HDFSUtil {

	/**
	 * 获取文件系统
	 * @param url
	 * @return
	 */
	public static FileSystem getFileSystem(String url) {
		// StringUtils中方法的操作对象是java.lang.String类型的对象，是JDK提供的String类型操作方法的补充
		if (StringUtils.isBlank(url)) {
			// 判断某字符串是否为空或长度为0或由空白符(whitespace)构成
			return null;
		}
		Configuration conf = new Configuration();
		FileSystem fs = null;
		try {
			URI uri = new URI(url.trim());
			fs = FileSystem.get(uri, conf);
		} catch (URISyntaxException | IOException e) {
			System.out.println(e);
		}
		return fs;
	}

	/**
	 * 获取文件系统
	 * @param url
	 * @param user
	 * @return
	 */
	public static FileSystem getFileSystem(String url, String user) {
		if (StringUtils.isBlank(url)) {
			return null;
		}
		Configuration conf = new Configuration();
		FileSystem fs = null;
		try {
			URI uri = new URI(url.trim());
			fs = FileSystem.get(uri, conf, user);
		} catch (InterruptedException | URISyntaxException | IOException e) {
			System.out.println(e);
		}
		return fs;
	}

	/**
	 * 创建目录
	 * @param path
	 * @return
	 * @throws Exception
	 */
	public static boolean mkdir(String path) throws Exception {
		FileSystem fs = getFileSystem(path, "root");
		boolean b = fs.mkdirs(new Path(path));
		fs.close();
		return b;
	}

	/**
	 * 读文件
	 * 
	 * @param filePath
	 * @throws IOException
	 */
	public static void readFile(String filePath) throws IOException {
		FileSystem fs = getFileSystem(filePath);
		InputStream in = null;
		try {
			in = fs.open(new Path(filePath));
			IOUtils.copyBytes(in, System.out, 4096, false);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {
			IOUtils.closeStream(in);
		}
	}

	/**
	 * 上传文件
	 * 
	 * @param src
	 * @param dst
	 * @throws IOException
	 */
	public static void putFile(String localPath, String hdfsPath) throws IOException {
		FileSystem fs = getFileSystem(hdfsPath, "root");
		fs.copyFromLocalFile(new Path(localPath), new Path(hdfsPath));
		fs.close();
	}

	/**
	 * 下载文件
	 * 
	 * @param hdfsPath
	 * @param localPath
	 * @throws IOException
	 */
	public static void getFile(String hdfsPath, String localPath) throws IOException {
		FileSystem fs = getFileSystem(hdfsPath, "root");
		Path hdfs_path = new Path(hdfsPath);
		Path local_path = new Path(localPath);
		fs.copyToLocalFile(hdfs_path, local_path);
		fs.close();
	}

	/**
	 * 递归删除
	 * 
	 * @param hdfsPath
	 * @return
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	public static boolean deleteFile(String hdfsPath) throws IllegalArgumentException, IOException {
		FileSystem fs = getFileSystem(hdfsPath, "root");
		return fs.delete(new Path(hdfsPath), true);
	}

	/**
	 * 目录列表
	 * 
	 * @param hdfsPath
	 * @return
	 */
	public static String[] listFile(String hdfsPath) {
		String[] files = new String[0];
		FileSystem fs = getFileSystem(hdfsPath, "root");
		Path path = new Path(hdfsPath);
		FileStatus[] st = null;
		try {
			st = fs.listStatus(path);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		files = new String[st.length];
		for (int i = 0; i < st.length; i++) {
			files[i] = st[i].toString();
		}
		return files;
	}

	/**
	 * 主方法，测试
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		String base = "hdfs://192.168.80.131:9000/";
		HDFSUtil.mkdir(base + "util");
		HDFSUtil.putFile("D:\\words", base + "util/");
		HDFSUtil.readFile(base + "util/words/words.txt");
		HDFSUtil.getFile(base + "util/words", "D:\\util");
		HDFSUtil.deleteFile(base + "abc");
	}

}
