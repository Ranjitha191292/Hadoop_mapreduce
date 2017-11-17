import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PurSale 
{

	public static class SaleMapper extends Mapper<LongWritable, Text, Text, Text> 
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String record = value.toString();
			String[] parts = record.split(",");
			context.write(new Text(parts[0]), new Text("s\t" + parts[1]));
		}
	}

	public static class PurMapper extends Mapper<LongWritable, Text, Text, Text> 
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String record = value.toString();
			String[] parts = record.split(",");
			context.write(new Text(parts[0]), new Text("p\t" + parts[1]));
		}
	}

	public static class Join extends Reducer<Text, Text, Text, Text> 
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			
			double total = 0.0;
			double total1 = 0.0;
			
			
			
			for (Text t : values) 
			{
				String parts[] = t.toString().split("\t");
				if (parts[0].equals("s")) 
				{
					total += Float.parseFloat(parts[1]);
				} 
				else if (parts[0].equals("p")) {
					total1 += Float.parseFloat(parts[1]);
				}
			}
			String str = String.format("%f\t%f", total, total1);
			context.write(key, new Text(str));
		}
	}

	public static void main(String[] args) throws Exception 
	{
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
	    job.setJarByClass(PurSale.class);
	    job.setJobName("Reduce Side Join");
		job.setReducerClass(Join.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setNumReduceTasks(1);
		MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,SaleMapper.class);
		MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,PurMapper.class);
		
		Path outputPath = new Path(args[2]);
		FileOutputFormat.setOutputPath(job, outputPath);
		//outputPath.getFileSystem(conf).delete(outputPath);
		
		job.waitForCompletion(true);
	}
}
