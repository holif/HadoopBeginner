package xyz.baal.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FlowCount {

	public static class FlowCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException{
			
			String line = value.toString();
			String[] records = line.split("\t");
			String month = records[1].substring(11, 13);
			long flow = Long.parseLong(records[3]);
			
			context.write(new Text(month), new LongWritable(flow));
		}
	}
	
	public static class FlowCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		public void reduce(Text key,Iterable<LongWritable> value,Context context) throws IOException, InterruptedException{
			long sum = 0;
			for (LongWritable longWritable : value) {
				sum += longWritable.get();
			}
			context.write(key, new LongWritable(sum));
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf,"FlowCount");
		job.setJarByClass(FlowCount.class);
		
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);
		
		job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        
		job.setInputFormatClass(ExcelInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
