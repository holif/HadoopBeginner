/*
找共同朋友，数据格式如下
A B C D E F
B A C D E
C A B E
D A B E
E A B C D
F A
第一字母表示本人，其他是他的朋友，找出有共同朋友的人，和共同朋友是谁

答案如下
AB E:C:D
AC E:B
AD B:E
AE C:B:D
BC A:E
BD A:E
BE C:D:A
BF A
CD E:A:B
CE A:B
CF A
DE B:A
DF A
EF A
*/
import java.io.IOException;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FindFriend {

	public static class ChangeMapper extends Mapper<Object, Text, Text, Text> {
		
		@Override
		public void map(Object key, Text value, Context context) throws
		IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			Text owner = new Text();
			Set<String> set = new TreeSet<String>();
			owner.set(itr.nextToken());
			while (itr.hasMoreTokens()) {
				set.add(itr.nextToken());
			}
			String[] friends = new String[set.size()];
			friends = set.toArray(friends);
			for(int i=0; i<friends.length; i++) {
				for(int j=i+1; j<friends.length; j++) {
					String outputkey = friends[i]+friends[j];
					context.write(new Text(outputkey),owner);
				}
			}
		}
	}

	public static class FindReducer extends Reducer<Text,Text,Text,Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws 
		IOException,InterruptedException {
			
			String commonfriends ="";
			for (Text val : values) {
				if(commonfriends == "") {
					commonfriends = val.toString();
				} else {
					commonfriends =
					commonfriends+":"+val.toString();
				}
			}
			context.write(key, new Text(commonfriends));
		}
		
	}

	public static void main(String[] args) throws IOException, 
	InterruptedException, ClassNotFoundException {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("args error");
			System.exit(2);
		}
		Job job = new Job(conf, "FindFriend");
		job.setJarByClass(FindFriend.class);
		job.setMapperClass(ChangeMapper.class);
		job.setCombinerClass(FindReducer.class);
		job.setReducerClass(FindReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}