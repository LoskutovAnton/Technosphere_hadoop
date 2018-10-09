import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DocumentCountJob extends Configured implements Tool {

    public static class DocumentCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        static final IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, Boolean> flags = new HashMap<>();

            String line = value.toString().toLowerCase();

            String fixedRegex = "\\p{L}+";

            Matcher m = Pattern.compile(fixedRegex).matcher(line);

            while (m.find()) {
                String word = m.group();

                if (!flags.containsKey(word)) {
                    flags.put(word, true);
                    context.write(new Text(word), one);
                }
            }
        }
    }

    public static class DocumentCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text word, Iterable<IntWritable> nums, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for(IntWritable i: nums) {
                sum += i.get();
            }

            context.write(word, new IntWritable(sum));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = GetJobConf(getConf(), args[0], args[1]);
        if (System.getProperty("mapreduce.input.indexedgz.bytespermap") != null) {
            throw new Exception("Property = " + System.getProperty("mapreduce.input.indexedgz.bytespermap"));
        }
        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static Job GetJobConf(Configuration conf, final String input, String output) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(DocumentCountJob.class);
        job.setJobName(DocumentCountJob.class.getCanonicalName());

        job.setInputFormatClass(DeflateInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output));

        FileSystem fs = new Path("/").getFileSystem(conf);

        RemoteIterator<LocatedFileStatus> fileListItr = fs.listFiles(new Path(input), false);
        while (fileListItr != null && fileListItr.hasNext()) {
            LocatedFileStatus file = fileListItr.next();
            if (file.getPath().getName().endsWith("pkz")) {
                FileInputFormat.addInputPath(job, file.getPath());
            }
        }

        job.setMapperClass(DocumentCountMapper.class);
        job.setCombinerClass(DocumentCountReducer.class);
        job.setReducerClass(DocumentCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new DocumentCountJob(), args);
        System.exit(exitCode);
    }
}