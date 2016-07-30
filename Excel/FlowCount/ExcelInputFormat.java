package xyz.baal.mr;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ExcelInputFormat extends FileInputFormat<LongWritable, Text>{

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		return new ExcelRecordReader();
	}
	
	public class ExcelRecordReader extends RecordReader<LongWritable, Text>{
		private LongWritable key = new LongWritable(-1);
		private Text value = new Text();
		private InputStream inputStream;//文件输入流
		private String[] strArray;//解析结果数组
		
		@Override
		public void close() throws IOException {
			if(inputStream!=null){
				inputStream.close();
			}
		}

		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return 0;
		}

		@Override
		public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {

			FileSplit split = (FileSplit) arg0;

			Configuration job = arg1.getConfiguration();
			
			Path filePath = split.getPath();
			
			FileSystem fileSystem = filePath.getFileSystem(job);
			
			inputStream = fileSystem.open(split.getPath());
			
			strArray = ExcelDeal.readExcel(inputStream);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			int next = (int) key.get() + 1;
			if(next<strArray.length&&strArray[next]!=null){
				key.set(next);
				value.set(strArray[next]);
				return true;
			}
			return false;
		}		
	}
}