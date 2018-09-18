package edu.rylynn.itemcf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URI;


/*
input:
1,101,5.0
1,102,3.0
1,103,2.5
2,101,2.0
2,102,2.5
......

output:
1 103:3 101:5 102:3
2 101:2 102:3 103:5 104:2
3 107:5 101:2 104:4 105:5
......

 */
public class ItemCFMROne {
    public static class ItemCFMapperOne extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] splits = line.split(",");
            String user = splits[0];
            String item = splits[1];
            String score = splits[2];
            String outputValue = item+":"+score;
            context.write(new Text(user), new Text(outputValue));
        }
    }

    public static class ItemCFReducerOne extends Reducer<Text, Text, Text, Text>
    {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for(Text value: values){
                sb.append(value);
                sb.append(" ");
            }
            String itemsLine = sb.toString();
            context.write(key, new Text(itemsLine));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //String inputPath = args[0];
        //String outputPath = args[1];
        Configuration conf = new Configuration();
        //conf.set("fs.defaultFS","hdfs://10.113.9.116:9000");
        conf.set("mapreduce.jobtracker.address", "10.113.9.116:8021");
        Job job = Job.getInstance(conf, "ItemCFMROne");

        job.setJarByClass(ItemCFMROne.class);
        job.setMapperClass(ItemCFMapperOne.class);
        job.setReducerClass(ItemCFReducerOne.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path outputPath = new Path("hdfs://10.113.9.116:9000/data/itemcf/output1");
        FileSystem fs = outputPath.getFileSystem(conf);
        if(fs.exists(outputPath)){
            fs.delete(outputPath, true);
        }
        FileInputFormat.addInputPath(job, new Path("hdfs://10.113.9.116:9000/data/itemcf/input"));
        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
