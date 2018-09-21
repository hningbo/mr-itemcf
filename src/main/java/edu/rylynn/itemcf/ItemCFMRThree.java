package edu.rylynn.itemcf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.htrace.fasterxml.jackson.databind.util.ArrayIterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/*
this file give a mapreduce implement of
matrix multiplication

matrix A(IxJ) multiply matrix B(JxK)

C(IxK)=AB

In mapper:

if the data from matrix A, we write into buffer with the format ((i, j),(ai, ak, 'A'))
and if from matrix B, we write with ((i, j),(bk, bj, 'B'), which ((i, j),(ai, ak, 'A')) is equals to ((i, j),(bk, bj, 'B')

In reducer:
a reducer recieve the data from mapper such as ((i, j),(ai, a1, 'A')),((i, j),(ai, a2, 'A')),
((i, j),(b1, bj, 'B')) .... which means that this reducer compute the C(i, j), and if ak == bk, mutiply them and sum up,
we can get the result.


*/
public class ItemCFMRThree {

    private static final int MATRIX_I = 7;
    private static final int MATRIX_K = 6;

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "item_cf3");


        job.setJarByClass(ItemCFMRThree.class);
        job.setMapperClass(ItemCFMapperThree.class);
        job.setReducerClass(ItemCFReducerThree.class);

        job.setMapOutputKeyClass(ItemCFMRTwo.ItemPair.class);
        job.setMapOutputValueClass(MatrixItem.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        Path outputPath = new Path("hdfs://10.113.9.116:9000/data/itemcf/output3");
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }


        FileInputFormat.addInputPath(job, new Path("hdfs://10.113.9.116:9000/data/itemcf/input"));
        FileInputFormat.addInputPath(job, new Path("hdfs://10.113.9.116:9000/data/itemcf/output2"));
        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class MatrixItem implements WritableComparable<MatrixItem> {

        private int i;
        private double value;
        private String matrixFrom;

        public MatrixItem() {
            super();
        }

        public MatrixItem(int i, double value, String matrixFrom) {
            this.i = i;
            this.value = value;
            this.matrixFrom = matrixFrom;
        }

        int getI() {
            return i;
        }

        void setI(int i) {
            this.i = i;
        }

        double getValue() {
            return value;
        }

        void setValue(double value) {
            this.value = value;
        }

        String getMatrixFrom() {
            return matrixFrom;
        }

        void setMatrixFrom(String matrixFrom) {
            this.matrixFrom = matrixFrom;
        }

        @Override
        public int compareTo(MatrixItem o) {
            if (Integer.compare(i, o.i) == 0) {
                return Double.compare(value, o.value);
            }
            return Integer.compare(i, o.i);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(i);
            dataOutput.writeDouble(value);
            dataOutput.writeUTF(matrixFrom);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            i = dataInput.readInt();
            value = dataInput.readDouble();
            matrixFrom = dataInput.readUTF();
        }
    }

    public static class ItemCFMapperThree extends Mapper<LongWritable, Text, ItemCFMRTwo.ItemPair, MatrixItem> {
        private MatrixItem matrixItem = new MatrixItem();
        private ItemCFMRTwo.ItemPair itemPair = new ItemCFMRTwo.ItemPair();
        private String line;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filePath = fileSplit.getPath().toString();


            if (filePath.contains("output2")) {
                /*
                data format:
                    101:101	5
                    101:102	3
                    101:103	4
                    101:104	4
                    101:105	2
                    101:106	2
                    101:107	1
                    102:101	3
                 */

                line = value.toString();
                String[] matrixIJV = line.split("\t");
                String[] matrixIJ = matrixIJV[0].split(":");
                int ai = Integer.parseInt(matrixIJ[0]);
                int aj = Integer.parseInt(matrixIJ[1]);
                double v = Double.parseDouble(matrixIJV[1]);
                matrixItem.setI(aj);
                matrixItem.setValue(v);
                matrixItem.setMatrixFrom("A");
                for (int k = 1; k <= MATRIX_K; k++) {
                    itemPair.setItemOne(ai);
                    itemPair.setItemTwo(k);
                    context.write(itemPair, matrixItem);
                }
            }
            if (filePath.contains("input")) {
                /*
                data format:
                    1,101,5.0
                    1,102,3.0
                    1,103,2.5
                    2,101,2.0
                    2,102,2.5
                 */
                line = value.toString();
                String[] matrixJKV = line.split(",");
                int bk = Integer.parseInt(matrixJKV[0]);
                int bj = Integer.parseInt(matrixJKV[1]);
                double v = Double.parseDouble(matrixJKV[2]);
                matrixItem.setI(bj);
                matrixItem.setValue(v);
                matrixItem.setMatrixFrom("B");
                for (int i = 1; i <= MATRIX_I; i++) {
                    itemPair.setItemOne(i + 100);
                    itemPair.setItemTwo(bk);
                    context.write(itemPair, matrixItem);
                }

            }
        }
    }

    public static class ItemCFReducerThree extends Reducer<ItemCFMRTwo.ItemPair, MatrixItem, Text, DoubleWritable> {


        @Override
        protected void reduce(ItemCFMRTwo.ItemPair key, Iterable<MatrixItem> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int itemOne = key.getItemOne();
            ArrayList<MatrixItem> valuesCache = new ArrayList<>();
            boolean flag = false;       // wheather the item has been bought by the user
            for(MatrixItem value: values){
                MatrixItem tmp = new MatrixItem();
                tmp.setI(value.i);
                tmp.setMatrixFrom(value.matrixFrom);
                tmp.setValue(value.value);
                valuesCache.add(tmp);
            }

            int valueSize = valuesCache.size();
            for(int i = 0; i<valueSize; i++){
                if(valuesCache.get(i).getMatrixFrom().equals("B") && itemOne ==  valuesCache.get(i).getI()){
                    flag = true;
                }
                for(int j = 0; j<valueSize; j++){
                    if (!valuesCache.get(i).getMatrixFrom().equals(valuesCache.get(j).getMatrixFrom()) && valuesCache.get(i).getI() == valuesCache.get(j).getI()) {
                        sum += valuesCache.get(i).getValue() * valuesCache.get(j).getValue();
                    }
                }
            }

            sum/=2;
            String outputKey;
            if(!flag){
                outputKey = String.valueOf(key.getItemTwo()) + " " + String.valueOf(key.getItemOne());
            }
            else{
                outputKey = String.valueOf(key.getItemTwo()) + " " + String.valueOf(key.getItemOne())+" 已购买";
            }

            context.write(new Text(outputKey), new DoubleWritable(sum));
        }
    }
}



