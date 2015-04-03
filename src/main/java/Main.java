public class Main {
	public static void main(String[] args) {
		WriterAndReader mWriterAndReader = new WriterAndReader();
		mWriterAndReader.writeToSequenceFile("a");
		mWriterAndReader.readSequenceFile();
	}
}
