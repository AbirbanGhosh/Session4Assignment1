package TVSalesData;



import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Driver {
	public static void main(String []args) throws Exception
	{
		Configuration conf = new Configuration();
		
		
		Job job = Job.getInstance(conf, "TVSalesData");
		
		job.setJarByClass(TVSalesData.Driver.class);
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setMapperClass(TVSalesData.MapperTask.class);
		
		// No reduce task needed
		job.setNumReduceTasks(0);
		
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path (args[1]));
		
		job.waitForCompletion(true);
	}

}
