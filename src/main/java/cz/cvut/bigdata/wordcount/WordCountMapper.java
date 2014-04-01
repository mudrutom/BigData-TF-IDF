package cz.cvut.bigdata.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.cz.CzechStemmer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

import java.io.IOException;

/**
 * Receives (byteOffsetOfLine, textOfLine), note we do not
 * care about the type of the key because we do not use it
 * anyway, and emits (word, 1) for each occurrence of the
 * word in the line of text (i.e. the received value).
 */
public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final CzechAnalyzer analyzer = new CzechAnalyzer(Version.LUCENE_47);
	private final CzechStemmer stemmer = new CzechStemmer();
	private final IntWritable ONE = new IntWritable(1);
	private final Text word = new Text();

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		TokenStream tokenStream = null;
		try {
			// instantiate and reset the token stream
			tokenStream = analyzer.tokenStream("word", value.toString());
			tokenStream.reset();

			// store the attribute
			final CharTermAttribute termAttribute = tokenStream.getAttribute(CharTermAttribute.class);
			// process all tokens
			while (tokenStream.incrementToken()) {
				int len = stemmer.stem(termAttribute.buffer(), termAttribute.length());
				String term = termAttribute.toString().substring(0, len);
				word.set(term);
				context.write(word, ONE);
			}
		} finally {
			// clean-up
			finish(tokenStream);
		}
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
