import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Offence {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,IntWritable>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	             String[] str = value.toString().split(",");
	             int speed = Integer.parseInt(str[1]);
	             context.write(new Text(str[0]),new IntWritable(speed));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
	  public static class ReduceClass extends Reducer<Text,IntWritable,Text,Text>
	   {
		    //private IntWritable result = new IntWritable(0);
		    int offence_percent = 0;
		    
		    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		    	int offence_count = 0;
			    int total = 0;
			  	for (IntWritable val : values)
		         {  
		        	if (val.get() > 65) 
		        	{
		        		offence_count++;
		        	}	
		        	total++;  	
		         }
		      offence_percent = (offence_count*100/total);  
		      String percentvalue = String.format("%d", offence_percent);
		      String valwithsign = percentvalue + "%" ;
		      //result.set(offence_percent);		      
//		      context.write(key, result);
		      context.write(key, new Text(valwithsign));
		    }
	   }
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "Offence percentage");
		    job.setJarByClass(Offence.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    //job.setNumReduceTasks(2);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(IntWritable.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}
