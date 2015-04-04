import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * Created by yizhao on 4/3/15.
 */
public class MergeSequenceFiles {
//    public static class mMapper extends Mapper<LongWritable, Text, Text, Text> {
//        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            String line = value.toString();
//            context.write(new Text(line), new Text(""));
//        }
//    }
//
//    public static class mReducer extends Reducer<Text, Text, Text, Text> {
//        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            context.write(key, new Text(""));
//        }
//    }


    public static void main(String[] args) throws Exception {
        String hadoopFS = "hdfs://localhost:9000/";
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hadoopFS);

        Job job = new Job(conf);
        job.setJobName("MergeSequenceFiles");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//        job.setMapperClass(mMapper.class);
//        job.setCombinerClass(mReducer.class);
//        job.setReducerClass(mReducer.class);
        job.setMapperClass(Mapper.class);
        job.setCombinerClass(Reducer.class);
        job.setReducerClass(Reducer.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path("/fuhu/logs/kafka/logging_consumer/temp"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/yizhao/Desktop/output"));
        job.waitForCompletion(true);
    }
}
