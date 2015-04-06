import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by yizhao on 4/3/15.
 */
public class MergeSequenceFiles {
    public static void mergeFile() {
        String hadoopFS = "hdfs://localhost:9000/";
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hadoopFS);

        String srcPath = "/fuhu/logs/kafka/logging_consumer/temp";
        String dstPath = "/fuhu/logs/kafka/logging_consumer/mergedOutput/mergedFile.log";

        try {
            FileSystem hdfs = FileSystem.get(conf);
            FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, conf, null);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws Exception {
        mergeFile();
    }
}
