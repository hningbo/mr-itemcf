# mr-itemcf


hive:



1)查询2014年12月10日到2014年12月13日有多少人浏览了商品
 select count(*) from full_shopping where time >= to_date('2014-12-10') and time <= to_date('2014-12-13');
 OK
 3855684
 
2)以天为统计单位，依次显示每天网站卖出去的商品的个数
 select time,count(*) count from full_shopping group by time order by time;
 
 OK
 2014-11-18      684628
 2014-11-19      687528
 2014-11-20      672189
 2014-11-21      634122
 2014-11-22      668509
 2014-11-23      722978
 2014-11-24      718217
 2014-11-25      699413
 2014-11-26      679323
 2014-11-27      689855
 2014-11-28      658806
 2014-11-29      684442
 2014-11-30      751093
 2014-12-01      744363
 2014-12-02      753810
 2014-12-03      788689
 2014-12-04      745391
 2014-12-05      693593
 2014-12-06      732821
 2014-12-07      763498
 2014-12-08      753138
 2014-12-09      767838
 2014-12-10      788712
 2014-12-11      944979
 2014-12-12      1344980
 2014-12-13      777013
 2014-12-14      779285
 2014-12-15      764085
 2014-12-16      751370
 2014-12-17      734520
 2014-12-18      711839


package edu.rylynn.itemcf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ItemCFMRThree {
    public static class ItemCFMapperThree extends Mapper<LongWritable, Text, Text, LongWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String filePath = fileSplit.getPath().toString();
            context.write(new Text(filePath), new LongWritable(1));
        }
    }

    public static class ItemCFReducerThree extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0L;
            for(LongWritable value: values){
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "item_cf2");


        job.setJarByClass(ItemCFMRThree.class);
        job.setMapperClass(ItemCFMapperThree.class);
        job.setReducerClass(ItemCFReducerThree.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        Path outputPath = new Path("hdfs://10.113.9.116:9000/data/itemcf/output3");
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }


        FileInputFormat.addInputPath(job, new Path("hdfs://10.113.9.116:9000/data/itemcf/output1"));
        FileInputFormat.addInputPath(job, new Path("hdfs://10.113.9.116:9000/data/itemcf/output2"));
        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
