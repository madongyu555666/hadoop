package org.hadoop.mapreduce.join;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * mapreduce的map任务
 * 
 * @author madongyu-ds
 *
 */
public class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {

	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		 //当前读取文件的路径
        String filePath=((FileSplit)context.getInputSplit()).getPath().toString();
        String joinKey="";
        String joinValue="";
        String fileFlag="";
        String[] array=value.toString().split(",");
      //判定当前行数据来自哪个文件
        if(filePath.contains("dept.txt")){
            fileFlag="l";//left
            joinKey=array[0];//部门编号
            joinValue=array[1];//部门名
        }else if(filePath.contains("emp.txt")){
        	fileFlag="r";//right
            joinKey=array[array.length-1];//部门编号
            joinValue=array[1];//雇员名
        }
        //输出键值对，并标记来源文件
        context.write(new Text(joinKey),new Text(joinValue+","+fileFlag));  
	}
}
