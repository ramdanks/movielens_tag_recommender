package com.bigdata.tagmap;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/** how to use by args: {searched tag} {input tags.csv} {input ratings.csv} {output mapred}  */
public class App
{
    public enum CONFIGKEY { TAG, MOVIEIDS }

    public static class TagMapper extends Mapper<Object, Text, IntWritable, IntWritable>
    {
        private final IntWritable zero = new IntWritable(0);
        private final IntWritable keyMovieId  = new IntWritable();
        @Override
        public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException
        {
            Configuration conf = context.getConfiguration();
            final String tag = conf.get(CONFIGKEY.TAG.name());

            String string = value.toString();
            Scanner rowScanner = new Scanner(string);
            while (rowScanner.hasNextLine())
            {
                TagHeader tagHeader = new TagHeader(rowScanner.nextLine());
                String objectTag = tagHeader.getTag();
                if (objectTag != null && objectTag.toLowerCase().contains(tag))
                {
                    keyMovieId.set(Integer.parseInt(tagHeader.getMovieId()));
                    context.write(keyMovieId, zero);
                }
            }
            rowScanner.close();
        }
    }

    public static class TagReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
    {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> value, Context context)
            throws IOException, InterruptedException
        {
            AtomicInteger counter = new AtomicInteger(0);
            value.forEach(o -> counter.incrementAndGet());
            context.write(key, new IntWritable(counter.get()));
        }
    }

    public static void main(String[] args) throws Exception
    {
        String tag = args[0].toLowerCase();
        System.out.println("Searched Tag:" + tag);

        Configuration conf = new Configuration();
        conf.set(CONFIGKEY.TAG.name(), tag);

        Job job = Job.getInstance(conf, "Movielens Tag Mapred");
        job.setJarByClass(App.class);
        job.setMapperClass(TagMapper.class);
        job.setReducerClass(TagReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        boolean success = job.waitForCompletion(false);
        System.out.println("Job Finished Status: " + (success ? "Success" : "Failed"));
        System.exit(success ? 0 : 1);
    }
}
