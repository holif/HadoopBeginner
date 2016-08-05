import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class SecondSort  extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new SecondSort(), args);

	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf);
		job.setJarByClass(getClass());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(SortMapper.class);

		job.setOutputKeyClass(MyPairWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setReducerClass(SortReducer.class);
		
		job.setSortComparatorClass(PairKeyComparator.class);
		
		job.waitForCompletion(true);
		return 0;
	}
}


class SortMapper extends Mapper<LongWritable, Text, MyPairWritable, NullWritable>{
	MyPairWritable pair= new MyPairWritable();
	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {		
		String[] strs = value.toString().split(" ");
		Text keyy = new Text(strs[0]);
		IntWritable valuee = new IntWritable(Integer.parseInt(strs[1]));
		pair.set(keyy, valuee);
		context.write(pair, NullWritable.get());
	};
}

class SortReducer extends Reducer<MyPairWritable, NullWritable,MyPairWritable, NullWritable>{
	protected void reduce(MyPairWritable key, java.lang.Iterable<NullWritable> values, Context context) throws IOException ,InterruptedException {
		context.write(key, NullWritable.get());
		
	};
}

class PairKeyComparator extends WritableComparator{

	public  PairKeyComparator() {
		super(MyPairWritable.class,true);
	}
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		MyPairWritable p1 = (MyPairWritable)a;
		MyPairWritable p2 = (MyPairWritable)b;
		if(!p1.getFirst().toString().equals(p2.getFirst().toString())){
			return p1.first.toString().compareTo(p2.first.toString());
		}else {
			return p1.getSecond().get() - p2.getSecond().get();
		}
	}
}

class MyPairWritable implements WritableComparable<MyPairWritable>{
	Text first;
	IntWritable second;
	
    public void set(Text first, IntWritable second){
        this.first = first;
        this.second = second;
    }
    public Text getFirst(){
        return first;
    }
    public IntWritable getSecond(){
        return second;
    }
	
	@Override
	public void readFields(DataInput in) throws IOException {
		first = new Text(in.readUTF());
		second = new IntWritable(in.readInt());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(first.toString());
		out.writeInt(second.get());
	}

	@Override
	public int compareTo(MyPairWritable o) {
		if(this.first != o.getFirst()){
			return this.first.toString().compareTo(o.first.toString());
		}else if(this.second != o.getSecond()){
			return this.second.get() - o.getSecond().get();
		}
		else	return 0;
	}
	
	@Override
	public String toString() {
		return first.toString() + " " + second.get();
	}
	
	@Override
	public boolean equals(Object obj) {
		MyPairWritable temp = (MyPairWritable)obj;
		return first.equals(temp.first) && second.equals(temp.second);
	}
	
	@Override
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}
}