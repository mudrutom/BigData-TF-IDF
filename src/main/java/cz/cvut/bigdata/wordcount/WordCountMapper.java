package cz.cvut.bigdata.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Receives (byteOffsetOfLine, textOfLine), note we do not
 * care about the type of the key because we do not use it
 * anyway, and emits (word, 1) for each occurrence of the
 * word in the line of text (i.e. the received value).
 */
public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final IntWritable ONE = new IntWritable(1);
	private final Text word = new Text();

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		final String[] words = value.toString().split(" ");
		for (String term : words) {
			word.set(term);
			context.write(word, ONE);
		}
	}
}
