package TVSalesData;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperTask extends Mapper<LongWritable, Text, NullWritable, Text>{
	
	
	@Override
	public void map(LongWritable key, Text value, Context con) 
			throws IOException, InterruptedException
	{
		String[] strArr = value.toString().split("\\|");
		
		// filter-out the invalid entry
		if(!strArr[0].equals("NA") && !strArr[1].equals("NA")){
			con.write(NullWritable.get(), value);
		}
		
	}
	

}
