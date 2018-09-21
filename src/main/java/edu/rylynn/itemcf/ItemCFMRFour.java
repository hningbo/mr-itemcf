package edu.rylynn.itemcf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ItemCFMRFour {
    public static class ItemCFMapperFour extends Mapper<Text, DoubleWritable, DoubleWritable, Text> {
        @Override
        protected void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {
            System.out.println(value.get());
            context.write(value, key);
        }
    }

    public static class ItemCFReducerFour extends Reducer<DoubleWritable, Text, Text ,DoubleWritable>{
        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value: values){
                context.write(value, key);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "item_cf4");

        job.setJarByClass(ItemCFMRFour.class);

        job.setMapperClass(ItemCFMapperFour.class);
        job.setReducerClass(ItemCFReducerFour.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        Path outputPath = new Path("hdfs://10.113.9.116:9000/data/itemcf/output4");
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        FileInputFormat.addInputPath(job, new Path("hdfs://10.113.9.116:9000/data/itemcf/output3"));
        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
