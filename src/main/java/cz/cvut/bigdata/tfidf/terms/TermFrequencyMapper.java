package cz.cvut.bigdata.tfidf.terms;

import cz.cvut.bigdata.tfidf.TermDocWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Receives <b>(line, 'text')</b> corresponding to the line
 * number and the document content. The mapper then parses
 * the text to get terms, using <i>Apache Lucene</i> analyzer,
 * then it will emit <b>(termDoc, 1)</b> pair for each term.
 */
public class TermFrequencyMapper extends Mapper<Text, Text, TermDocWritable, IntWritable> {

	// Lucene Czech analyzer:
	//  standard filter > lower case filter > stop filter > czech stem filter
	private final CzechAnalyzer analyzer = new CzechAnalyzer(Version.LUCENE_47);

	private final TermDocWritable termDoc = new TermDocWritable();
	private final IntWritable one = new IntWritable(1);

	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		for (String term : parseTerms(value.toString())) {
			// emit (termDoc, 1) pair
			termDoc.set(term, Integer.parseInt(key.toString()));
			context.write(termDoc, one);
		}
	}

	/** Parsing of terms from the document using Lucene. */
	protected List<String> parseTerms(String text) throws IOException {
		final List<String> result = new LinkedList<String>();

		// remove HTML tags and other special chars
		text = text.replaceAll("<(.*?)>", " ").replaceAll("[.,!?:;_'\"]", " ");

		TokenStream tokenStream = null;
		try {
			// instantiate and reset the token stream
			tokenStream = analyzer.tokenStream("word", text);
			tokenStream.reset();

			// store term attribute
			final CharTermAttribute termAttribute = tokenStream.getAttribute(CharTermAttribute.class);
			// process all tokens
			while (tokenStream.incrementToken()) {
				String term = termAttribute.toString();
				// exclude single characters and numbers
				if (term.length() > 1 && !term.matches("^(.*?)[0-9](.*?)$")) {
					result.add(term);
				}
			}
		} finally {
			// clean-up
			finish(tokenStream);
		}

		return result;
	}

	private static void finish(TokenStream tokenStream) throws IOException {
		if (tokenStream != null) {
			try {
				tokenStream.end();
			} finally {
				tokenStream.close();
			}
		}
	}
}
