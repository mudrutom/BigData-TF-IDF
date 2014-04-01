package cz.cvut.bigdata.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Receives (word, list[1, 1, 1, 1, ..., 1]) where the number of 1s
 * corresponds to the total number of times the word occurred in the
 * input text, which is precisely the value the reducer emits along
 * with the original word as the key.
 * <p/>
 * NOTE: The received list may not contain only 1s if a combiner is used.
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text text, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		context.write(text, new IntWritable(sum));
	}
}
