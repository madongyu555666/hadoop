package org.hadoop.mapreduce.twoorder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Sort2Reducer extends Reducer<Text, NullWritable, NullWritable, Text> {

	
	@Override
	protected void reduce(Text key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		context.write(NullWritable.get(),key);
	}
}
