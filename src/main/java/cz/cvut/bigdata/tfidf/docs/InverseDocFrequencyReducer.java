package cz.cvut.bigdata.tfidf.docs;

import cz.cvut.bigdata.tfidf.TermDocFreqWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Receives <b>(term, list[termDocFreq])</b> where the list
 * corresponds to the document frequencies for given term.
 * <p/>
 * First, the reducer will receive a pair with the key value
 * starting '_', that indicate the overall number of documents.
 * Then, the reducer will compute the TF-IDF score for each
 * term-document from the received list.
 * <p/>
 * The TF-IDF is computed as follows:
 * <pre>
 *     tfidf = tf * log(idf)
 *     idf = N / df
 * </pre>
 */
public class InverseDocFrequencyReducer extends Reducer<Text, TermDocFreqWritable, Text, DoubleWritable> {

	private final Text docTerm = new Text();
	private final DoubleWritable tfidf = new DoubleWritable();

	private int numberOfDocuments = 0;

	@Override
	protected void reduce(Text key, Iterable<TermDocFreqWritable> values, Context context) throws IOException, InterruptedException {
		if (key.toString().startsWith("_")) {
			// '_' indicates the number of documents
			numberOfDocuments = values.iterator().next().getFreq();
			return;
		}

		// copy all term-document-frequency values
		final LinkedList<TermDocFreqWritable> termDocFreq = new LinkedList<TermDocFreqWritable>();
		for (TermDocFreqWritable value : values) {
			termDocFreq.add(new TermDocFreqWritable(value));
		}

		// compute the TF-IDF score for all term-documents
		final int docFrequency = termDocFreq.size();
		for (TermDocFreqWritable value : termDocFreq) {
			docTerm.set(value.toString());
			tfidf.set(computeTFIDF(value, docFrequency));
			context.write(docTerm, tfidf);
		}
	}

	/** Computes the TF-IDF score for given term-document-frequency. */
	protected double computeTFIDF(TermDocFreqWritable termDocFreq, int docFrequency) {
		return (double) termDocFreq.getFreq() * Math.log((double) numberOfDocuments / docFrequency);
	}
}
