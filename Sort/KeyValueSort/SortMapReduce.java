import java.io.IOException;  
  
import org.apache.commons.lang.StringUtils;  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.NullWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
  
public class SortMapReduce {  
  
    public static class SortMapper extends  
            Mapper<LongWritable, Text, Bean, NullWritable> {  
        @Override  
        protected void map(  
                LongWritable k1,  
                Text v1,  
                Mapper<LongWritable, Text, Bean, NullWritable>.Context context)  
                throws IOException, InterruptedException {  
              
            String line = v1.toString();  
            String[] fields = StringUtils.split(line, "\t");  
            String carName = fields[0];
            long sum = Long.parseLong(fields[1]);  
  
            context.write(new Bean(carName,sum),NullWritable.get());  
        }  
    }  
  
    public static class SortReducer extends  
            Reducer<Bean, NullWritable, Text, Bean> {  
        @Override  
        protected void reduce(Bean k2, Iterable<NullWritable> v2s,  
                Reducer<Bean, NullWritable, Text, Bean>.Context context)  
                throws IOException, InterruptedException {  
				
            String carName = k2.getCarName();  
            context.write(new Text(carName), k2);  
			
        }  
    }  
  
    public static void main(String[] args) throws IOException,  
            ClassNotFoundException, InterruptedException {  
  
        Configuration conf = new Configuration();  
        Job job = Job.getInstance(conf);  
  
        job.setJarByClass(SortMapReduce.class);  
  
        job.setMapperClass(SortMapper.class);  
        job.setReducerClass(SortReducer.class);  
  
        job.setMapOutputKeyClass(Bean.class);  
        job.setMapOutputValueClass(NullWritable.class);  
  
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(Bean.class);  
  
        FileInputFormat.setInputPaths(job, new Path(args[0]));  
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  
  
        System.exit(job.waitForCompletion(true) ? 0 : 1);  
    }  
}  