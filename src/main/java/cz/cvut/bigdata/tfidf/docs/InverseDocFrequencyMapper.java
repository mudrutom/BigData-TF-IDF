package cz.cvut.bigdata.tfidf.docs;

import cz.cvut.bigdata.tfidf.TermDocFreqWritable;
import cz.cvut.bigdata.tfidf.TermDocWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Receives <b>(termDoc, frequency)</b> that represent the
 * frequency of the term in the document. The mapper just
 * performs transformation to <b>(term, termDocFreq)</b>
 * pairs. When the '_' key value is received, indicating
 * the overall number of documents, the mapper will emit
 * special key-value pair for each reducer.
 */
public class InverseDocFrequencyMapper extends Mapper<Text, Text, Text, TermDocFreqWritable> {

	private final Text term = new Text();
	private final TermDocWritable termDoc = new TermDocWritable();
	private final TermDocFreqWritable termDocFreq = new TermDocFreqWritable();

	private int reducersNum;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		reducersNum = context.getNumReduceTasks();
	}

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		termDoc.parse(key.toString());
		if (termDoc.getTerm().equals("_")) {
			// key '_' indicates the number of documents
			for (int i = 0; i < reducersNum; i++) {
				// send it to each reducer
				term.set("_" + i);
				termDocFreq.set("_", i, Integer.parseInt(value.toString()));
				context.write(term, termDocFreq);
			}
			return;
		}

		// emit the result
		term.set(termDoc.getTerm());
		termDocFreq.copy(termDoc);
		termDocFreq.setFreq(Integer.parseInt(value.toString()));
		context.write(term, termDocFreq);
	}
}
