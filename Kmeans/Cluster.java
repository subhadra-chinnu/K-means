package com.subbu;

import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Cluster {

    // five data sets are taken
    public static Double[][] dsets = new Double[5][2];
    public static Double[][] centroid = new Double[2][2];

    // two kmean clusters
    public static Double[][] clustercheck0 = new Double[10][2];
    public static Double[][] clustercheck1 = new Double[10][2];

    public final static IntWritable zero = new IntWritable(0);
    public final static IntWritable one = new IntWritable(1);

    public static int iter = 0;
    public static int clusterIndex = 0;

    //input and output paths
    public static final Path inputFile = new Path("input/data.hdfs");
    public static final String outputDirString = "output";
    public static final Path outputDir = new Path(outputDirString);
    public static final String centroidsDir = "ceninput";
    public static final Path centroidsFile = new Path("ceninput/centroids");

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        do {
            final Configuration con = new Configuration();
            final FileSystem fsobj = FileSystem.get(con);
            final Job job = Job.getInstance(con, "KMeans");

            job.setMapperClass(ClusterMapper.class);
            job.setReducerClass(ClusterReducer.class);
            job.setJarByClass(Cluster.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, inputFile);

            if (fsobj.exists(outputDir)) {
                fsobj.delete(outputDir, true);
            }

            FileOutputFormat.setOutputPath(job, outputDir);
            boolean status = job.waitForCompletion(true);

            System.out.println("Completed iter: " + iter + ", status: " + status);

            if(status){
                FileUtil.copy(fsobj, new Path(outputDirString + "/part-r-00000"), fsobj, new Path(centroidsDir + "/iter" + iter), false, con);
                iter++;
                clusterIndex++;
            } else {
                System.exit(1);
            }


        } while (iter == 0
                || !clustercheck0[iter - 1][0].equals(clustercheck0[iter][0])
                || clustercheck1[iter - 1][1].equals(clustercheck1[iter][1]));
    }

    // mapper function
    public static class ClusterMapper extends
            Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void setup(
                Mapper<LongWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {

            System.out.println("Mapper setup");

            int Index = 0;

            Configuration con = context.getConfiguration();
            FileSystem fsobj = FileSystem.get(con);

            Path centroidsPath ;
            if(iter == 0 ){
                centroidsPath = centroidsFile;
            } else {
                centroidsPath = new Path(centroidsDir + "/iter" + (iter - 1));
            }

            BufferedReader reader = new BufferedReader(new InputStreamReader(fsobj.open(centroidsPath)));
            String num;
            while ((num = reader.readLine()) != null) {
                //System.out.println(num.toString());
                String[] a = num.split("\\s+");
                int tem = Integer.parseInt(a[0]);
                for (tem = 0; tem <= 1; tem++) {
                    String[] value = a[1].split(",");
                    centroid[Index][0] = Double.parseDouble((value[0]));
                    centroid[Index][1] = Double.parseDouble(value[1]);
                }
                Index++;
            }
            super.setup(context);
            System.out.println("centroid: " + Arrays.deepToString(centroid));
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] num = value.toString().split(",");
            System.out.println("Inmapper: " + Arrays.toString(num));
            int itermapper = 0;
            dsets[itermapper][0] = Double.parseDouble(num[0]);
            dsets[itermapper][1] = Double.parseDouble(num[1]);

            double x0 = dsets[itermapper][0] - centroid[0][0];
            double y0 = dsets[itermapper][1] - centroid[0][1];
            double dsvec0 = Math.sqrt(x0 * x0 + y0 * y0);

            double x1 = dsets[itermapper][0] - centroid[1][0];
            double y1 = dsets[itermapper][1] - centroid[1][1];
            double dsvec1 = Math.sqrt(x1 * x1 + y1 * y1);

            String display = "" + dsets[itermapper][0] + ","
                    + dsets[itermapper][1];

            if (dsvec0 > dsvec1) {
                context.write(one, new Text(display));
            } else {
                context.write(zero, new Text(display));
            }
            itermapper++;
        }

    }

    public static class ClusterReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            double x0sum = 0;
            double y0sum = 0;
            double y1sum = 0;
            double x1sum = 0;
            int iter1 = 0;
            int iter2 = 0;

            if (key.equals(one)) {
                for (Text text : values) {
                    String num = text.toString();
                    String[] nums = num.split(",");
                    x1sum += Double.parseDouble(nums[0]);
                    y1sum += Double.parseDouble(nums[1]);
                    iter2++;
                }
                clustercheck1[iter][0] = x1sum / iter2;
                clustercheck1[iter][1] = y1sum / iter2;
                String num2 = x1sum / iter2 + "," + y1sum / iter2;
                context.write(one, new Text(num2));
                System.out.println("Label:" + key.toString());
                System.out.printf("clustercheck1 : [%s][0] = %s\n", clusterIndex, clustercheck1[clusterIndex][0]);
                System.out.printf("clustercheck1 : [%s][1] = %s\n", clusterIndex, clustercheck1[clusterIndex][1]);
            } else {
                for (Text text : values) {
                    String num = text.toString();
                    String[] nums = num.split(",");
                    x0sum += Double.parseDouble(nums[0]);
                    y0sum += Double.parseDouble(nums[1]);
                    iter1++;
                }
                clustercheck0[iter][0] = x0sum / iter1;
                clustercheck0[iter][1] = y0sum / iter1;
                String num1 = x0sum / iter1 + "," + y0sum / iter1;
                context.write(zero, new Text(num1));

                System.out.println("Label: " + key.toString());
                System.out.printf("clusterIndex: [%s][0] = %s\n", clusterIndex, clustercheck0[clusterIndex][0]);
                System.out.printf("clusterIndex: [%s][1] = %s\n", clusterIndex, clustercheck0[clusterIndex][1]);
            }
        }
    }
}
