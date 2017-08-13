package ca.lambton.bigdata.hadoop.wordcount;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
	
	public static void main(String [] args) throws Exception {

		Configuration c=new Configuration();
		Job j=new Job(c,"wordcount");
		j.setJarByClass(WordCount.class);
		j.setMapperClass(MapForWordCount.class);
		j.setReducerClass(ReduceForWordCount.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		//j.setNumReduceTasks(6); // Sets the number of reduce tasks (comment to use default (1))
		FileInputFormat.addInputPath(j, new Path(args[0]));
		FileOutputFormat.setOutputPath(j, new Path(args[1]));
		System.exit(j.waitForCompletion(true)?0:1);
	}

	public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable>{

		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

			String line = value.toString();
			String[] words=line.split(" ");
			for(String word: words ) {
				Text outputKey = new Text(word.toUpperCase().trim());
				IntWritable outputValue = new IntWritable(1);
				con.write(outputKey, outputValue);
			}

		}

	}

	public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {

			int sum = 0;
			for(IntWritable value : values) {
				sum += value.get();
			}
			if(sum>1000){
				con.write(word, new IntWritable(sum));
			}
		}

	}

}
