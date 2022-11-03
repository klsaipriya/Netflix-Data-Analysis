import java.io.*;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Netflix {
    /* ... */
    public static class MyMapper extends Mapper<Object,Text,IntWritable,IntWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            if(value.toString().endsWith(":"))
            {

            }
            else
            {
                int x = s.nextInt();
                int y = s.nextInt();
                context.write(new IntWritable(x),new IntWritable(y));
            }
            s.close();
        }
    }

    public static class MyReducer extends Reducer<IntWritable,IntWritable,IntWritable,DoubleWritable> {
        @Override
        public void reduce ( IntWritable key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
            double sum = 0.0;
            long count = 0;
            for (IntWritable v: values) {
                sum += v.get();
                count++;
            };
            double val=sum/count*10;
            double finalval=(int)((val*10)/10.0);
            context.write(key,new DoubleWritable(finalval));
        }
    }

    public static class MyMapperOne extends Mapper<Object,Text,DoubleWritable,IntWritable> {
    @Override
        public void map ( Object key, Text value, Context context )
        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter("\t");
            int x = s.nextInt();
            double y = s.nextDouble();
            int k=1;
            context.write(new DoubleWritable(y),new IntWritable(k));
            s.close();
        }
    }

    public static class MyReducerOne extends Reducer<DoubleWritable,IntWritable,DoubleWritable,IntWritable> {
    @Override
        public void reduce ( DoubleWritable key, Iterable<IntWritable> values, Context context )
        throws IOException, InterruptedException {
            int sum=0;
            for (IntWritable v: values) {
                sum += v.get();
            };
            context.write(new DoubleWritable(key.get()/10.0),new IntWritable(sum));
        }
    }

    public static void main ( String[] args ) throws Exception {

        Configuration conf1 = new Configuration();
        Job job = Job.getInstance(conf1, "JobOne");
        job.setJarByClass(Netflix.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        
        job.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job1 = Job.getInstance(conf2, "JobTwo");
        job1.setJarByClass(Netflix.class);
        job1.setOutputKeyClass(DoubleWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setMapOutputKeyClass(DoubleWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setMapperClass(MyMapperOne.class);
        job1.setReducerClass(MyReducerOne.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job1,new Path(args[1]));
        FileOutputFormat.setOutputPath(job1,new Path(args[2]));
        job1.waitForCompletion(true);
    }
}

