package cz.cvut.bigdata.wordcount;

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
 * Receives (byteOffsetOfLine, textOfLine), note we do not
 * care about the type of the key because we do not use it
 * anyway, and emits (word, 1) for each occurrence of the
 * word in the line of text (i.e. the received value).
 */
public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

	// Lucene Czech analyzer:
	//  standard filter > lower case filter > stop filter > czech stem filter
	private final CzechAnalyzer analyzer = new CzechAnalyzer(Version.LUCENE_47);

	private final IntWritable ONE = new IntWritable(1);
	private final Text word = new Text();

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		for (String term : parseTerms(value.toString())) {
			word.set(term);
			context.write(word, ONE);
		}
	}

	protected List<String> parseTerms(String text) throws IOException {
		final List<String> result = new LinkedList<String>();

		// remove HTML tags, dots and commas
		text = text.replaceAll("<.*?>", " ").replaceAll("[.,]", " ");

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
				if (term.length() > 1 && !term.matches("^\\d*$")) {
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
