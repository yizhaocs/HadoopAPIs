import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

public class WriterAndReader {
	private final static Logger log = LoggerFactory.getLogger(WriterAndReader.class);
	private final static Text EMPTY_KEY = new Text();
	// private static String hadoopFS = "hdfs://name01.hdp-dev-us-e.fuhu.org/";
	private static String hadoopFS = "hdfs://localhost:9000/";
	private static String sequenceFileName = "/fuhu/logs/kafka/logging_consumer/temp/";
	private static org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(sequenceFileName);
	private static SequenceFile.Writer writer = null;
	private static Configuration conf;

	static {
		conf = new Configuration();
		conf.set("fs.defaultFS", hadoopFS);
		try {
			writer = createWriter();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void writeToSequenceFile(String data) {
		if ((conf != null) && (data != null)) {
			BytesWritable value = new BytesWritable(data.getBytes());
			try {
				writer.append(EMPTY_KEY, value);
			} catch (IOException e) {
				e.printStackTrace();
			}

			try {
				closeWriter();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public void readSequenceFile() {
		String hadoopFS = "hdfs://localhost:9000/";
		String sequenceFileName = "/fuhu/logs/kafka/logging_consumer/temp/test.log";
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hadoopFS);
		org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(sequenceFileName);
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
			Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			while (reader.next(key, value)) {
				System.out.println("key : " + key + " - value : " + new String(value.getBytes()));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(reader);
		}
	}

	public void listHadoopConfiguration(Configuration conf) {
		int i = 0;
		System.out.println("------------------------------------------------------------------------------------------");
		Iterator<?> iterator = conf.iterator();
		while (iterator.hasNext()) {
			i++;
			iterator.next();
			System.out.println(i + " - " + iterator.next());
		}
		System.out.println("------------------------------------------------------------------------------------------");
	}

	// suffixTo) throws IOException
	private void closeWriter() throws IOException {
		IOUtils.closeStream(writer);
	}

	private static SequenceFile.Writer createWriter() throws IOException {
		// hadoop 2.x api for creating a sequence file writer
		path = new org.apache.hadoop.fs.Path(sequenceFileName + "test.log");
		return writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(path), SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(BytesWritable.class));
	}
}