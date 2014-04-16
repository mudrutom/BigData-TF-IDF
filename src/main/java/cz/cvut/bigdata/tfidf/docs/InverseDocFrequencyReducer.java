package cz.cvut.bigdata.tfidf.docs;

import cz.cvut.bigdata.tfidf.TermDocFreqWritable;
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
 * term-document from the received list. It filters out terms
 * occurring less then 3 times or more than N/2 times. Finally,
 * it emits the TF-IDF matrix in sparse representation.
 * <p/>
 * The TF-IDF is computed as follows:
 * <pre>
 *     tfidf = log(tf + 1) * log(idf)
 *     idf = N / df
 * </pre>
 */
public class InverseDocFrequencyReducer extends Reducer<Text, TermDocFreqWritable, Text, Text> {

	private final Text tfidf = new Text();

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

		final int docFrequency = termDocFreq.size();
		// filter out un-frequent and too-frequent terms
		if (docFrequency < 3 || docFrequency > numberOfDocuments / 2) {
			return;
		}

		// compute the TF-IDF score for all term-documents
		final StringBuilder tfidfLine = new StringBuilder();
		for (TermDocFreqWritable value : termDocFreq) {
			tfidfLine.append(value.getDoc()).append(':');
			tfidfLine.append(computeTFIDF(value, docFrequency));
			tfidfLine.append(' ');
		}
		tfidfLine.delete(tfidfLine.length() - 1, tfidfLine.length());

		// emit one sparse line of the TF-IDF matrix
		tfidf.set(tfidfLine.toString());
		context.write(key, tfidf);
	}

	/** Computes the TF-IDF score for given term-document-frequency. */
	protected double computeTFIDF(TermDocFreqWritable termDocFreq, int docFrequency) {
		return Math.log((double) termDocFreq.getFreq() + 1.0) * Math.log((double) numberOfDocuments / docFrequency);
	}
}
