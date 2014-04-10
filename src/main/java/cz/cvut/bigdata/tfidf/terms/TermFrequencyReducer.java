package cz.cvut.bigdata.tfidf.terms;

import cz.cvut.bigdata.tfidf.TermDocWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Receives <b>(termDoc, list[1,1,...,1])</b> where the numbers
 * corresponds to the number of times the term occurred in the
 * document. The reducer simply emits the sum of those numbers,
 * i.e. the term document frequency, if it is greater than 1.
 */
public class TermFrequencyReducer extends Reducer<TermDocWritable, IntWritable, Text, IntWritable> {

	private final Text termDoc = new Text();
	private final IntWritable frequency = new IntWritable();

	@Override
	public void reduce(TermDocWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		// sum-up term occurrences
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}

		if (sum > 1) {
			// emit term document frequency
			termDoc.set(key.toString());
			frequency.set(sum);
			context.write(termDoc, frequency);
		}
	}
}
