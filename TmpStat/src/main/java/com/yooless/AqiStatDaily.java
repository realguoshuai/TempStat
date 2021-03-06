package com.zhiyou100;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class AqiStatDaily {
	public static class StatMapper extends Mapper<Object, Text, Text, IntWritable> {
		private IntWritable intValue = new IntWritable();
		private Text dateKey = new Text();

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String[] items = value.toString().split(",");
			String date = items[0];
			String aqi = items[6];
			if (!"DATE".equals(date) && !"N/A".equals(aqi)) {
				dateKey.set(date);
				intValue.set(Integer.parseInt(aqi));
				context.write(dateKey, intValue);
			}
		}
	}

	public static class StatReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int aqi_sum = 0;
			int count = 0;
			for (IntWritable val : values) {
				aqi_sum += val.get();
				count++;
			}
			int aqi_avg = aqi_sum / count;
			result.set(aqi_avg);
			context.write(key, result);
		}
	}

	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");
			Job job = Job.getInstance(conf, "AqiStatDaily");
			job.setInputFormatClass(TextInputFormat.class);
			job.setJarByClass(AqiStatDaily.class);
			job.setMapperClass(StatMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setPartitionerClass(HashPartitioner.class);
			job.setReducerClass(StatReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			Path inputPath = new Path("hdfs://master:9000/data-40/beijing.txt");
			FileInputFormat.addInputPath(job, inputPath);

			Path outputDir = new Path("/data-40-02");
			FileOutputFormat.setOutputPath(job, outputDir);
			FileSystem.get(conf).delete(outputDir, true);
			boolean flag = job.waitForCompletion(true);
			System.out.println(flag ? "成功" : "失败");
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}
}