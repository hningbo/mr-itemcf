package edu.rylynn.itemcf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/*
input:
1 103:3 101:5 102:3
2 101:2 102:3 103:5 104:2
3 107:5 101:2 104:4 105:5
4 103:3 106:4 104:4 101:5
5 101:4 102:3 103:2 104:4 105:3 106:4
6 102:4 103:2 105:3 107:4

output:
101:101 5
101:102 3
101:103 4
101:104 4
.....
 */
public class ItemCFMRTwo {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ItemCFMRTwo");


        job.setJarByClass(ItemCFMRTwo.class);
        job.setMapperClass(ItemCFMapperTwo.class);
        job.setReducerClass(ItemCFReducerTwo.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputKeyClass(ItemPair.class);
        job.setOutputValueClass(LongWritable.class);

        Path outputPath = new Path("hdfs://10.113.9.116:9000/data/itemcf/output2");
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }


        FileInputFormat.addInputPath(job, new Path("hdfs://10.113.9.116:9000/data/itemcf/output1"));
        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class ItemCFMapperTwo extends Mapper<Text, Text, ItemPair, LongWritable> {
        ItemPair itemPair = new ItemPair();
        LongWritable one = new LongWritable(1);

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] items = line.split(" ");
            for (int i = 0; i < items.length; i++) {
                for (int j = 0; j < items.length; j++) {
                    String[] itemScore1 = items[i].split(":");
                    String[] itemScore2 = items[j].split(":");

                    int item1 = Integer.parseInt(itemScore1[0]);
                    int item2 = Integer.parseInt(itemScore2[0]);
                    item1 = Math.min(item1, item2);
                    item2 = Math.max(item1, item2);

                    itemPair.setItemOne(item1);
                    itemPair.setItemTwo(item2);
                    context.write(itemPair, one);
                }
            }
        }
    }

    public static class ItemCFReducerTwo extends Reducer<ItemPair, LongWritable, ItemPair, LongWritable> {
        @Override
        protected void reduce(ItemPair key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 1L;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    private static class ItemPair implements WritableComparable<ItemPair> {

        private int itemOne;
        private int itemTwo;

        public ItemPair() {
            super();
        }

        ItemPair(int itemOne, int itemTwo) {
            this.itemOne = itemOne;
            this.itemTwo = itemTwo;
        }

        public int getItemOne() {

            return itemOne;
        }

        public void setItemOne(int itemOne) {
            this.itemOne = itemOne;
        }

        public int getItemTwo() {
            return itemTwo;
        }

        public void setItemTwo(int itemTwo) {
            this.itemTwo = itemTwo;
        }

        @Override
        public String toString() {
            return String.valueOf(itemOne) + ":" + String.valueOf(itemTwo);
        }

        @Override
        public int compareTo(ItemPair o) {
            if (itemOne == o.getItemOne()) {
                return itemTwo - o.getItemTwo();
            } else {
                return itemOne - o.getItemOne();
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (obj.getClass() != ItemPair.class) {
                return false;
            }
            ItemPair item2 = (ItemPair) obj;
            if ((item2.getItemTwo() == itemTwo) && (item2.getItemOne() == itemOne)) {
                return true;
            } else {
                return false;
            }
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(itemOne);
            dataOutput.writeInt(itemTwo);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            itemOne = dataInput.readInt();
            itemTwo = dataInput.readInt();
        }
    }
}
