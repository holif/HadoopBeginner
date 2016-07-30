import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KPIBrowser {

    public static class KPIBrowserMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            KPI kpi = KPI.filterBroswer(value.toString());
            if (kpi.isValid()) {
                word.set(kpi.getHttp_user_agent());
                context.write(word, one);
            }
        }
    }

    public static class KPIBrowserReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {        
        	int sum = 0;
        	for (IntWritable val : values) {
        		sum += val.get();
        	} 
        	result.set(sum);
        	context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        
        @SuppressWarnings("deprecation")
		Job job = new Job(conf);
        job.setJarByClass(KPIBrowser.class);
       
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(KPIBrowserMapper.class);
        job.setCombinerClass(KPIBrowserReducer.class);
        job.setReducerClass(KPIBrowserReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
