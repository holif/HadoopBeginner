import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KPIIP {

    public static class KPIIPMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text word = new Text();
        private Text ips = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            KPI kpi = KPI.filterIPs(value.toString());
            if (kpi.isValid()) {
                word.set(kpi.getRequest());
                ips.set(kpi.getRemote_addr());
                context.write(word, ips);
            }
        }
    }

    public static class KPIIPReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        private Set<String> count = new HashSet<String>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           
        	Iterator<Text> iterator = values.iterator();
        	while (iterator.hasNext()) {
                count.add(iterator.next().toString());
            }
            result.set(String.valueOf(count.size()));
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
       
        @SuppressWarnings("deprecation")
		Job job = new Job(conf, "KPIIP");
        job.setJarByClass(KPIIP.class);
		
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setMapperClass(KPIIPMapper.class);
        job.setCombinerClass(KPIIPReducer.class);
        job.setReducerClass(KPIIPReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
