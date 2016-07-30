import java.io.IOException;
import java.text.ParseException;

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
public class KPITime {

    public static class KPITimeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    	
        private IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value,  Context context)  throws IOException, InterruptedException {
            KPI kpi = KPI.filterBroswer(value.toString());
            if (kpi.isValid()) {
                try {
					word.set(kpi.getTime_local_Date_hour());
				} catch (ParseException e) {
					e.printStackTrace();
				}
                context.write(word, one);
            }
        }
    }

    public static class KPITimeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
		Job job = new Job(conf, "KPITime");
        job.setJarByClass(KPITime.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(KPITimeMapper.class);
        job.setCombinerClass(KPITimeReducer.class);
        job.setReducerClass(KPITimeReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
