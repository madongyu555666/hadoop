package org.hadoop.mapreduce.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * reducer任务
 * 
 * @author madongyu-ds
 *
 */
public class JoinReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		 Iterator<Text> it=values.iterator();
	        String deptName="";
	        List<String> empNames=new ArrayList<>();
	        while(it.hasNext()){
	            //取一行记录
	        	String[] array=it.next().toString().split(",");
	            //判定当前记录来源于哪个文件，并根据文件格式解析记录获取相应的信息
	        	if("l".equals(array[1])){//只有1条记录的flag=l
	                deptName=array[0];
	        	}else if("r".equals(array[1])){
	                empNames.add(array[0]);
	            }
	        }
	      //求解笛卡尔积，对每个dept的1条记录与emp中多条记录作一次迭代
	        for(String en:empNames){
	            context.write(new Text(deptName), new Text(en));
	        }
	
   }
}