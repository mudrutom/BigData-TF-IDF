package cz.cvut.bigdata.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * (word, count) pairs are sent to reducers based on the length of the word.
 * <p/>
 * The maximum length of a word that will be processed by
 * the target reducer. For example, using 3 reducers the
 * word length span processed by the reducers would be:
 * <pre>
 * reducer     lengths processed
 * -------     -----------------
 * 00000           1 -- 14
 * 00001          15 -- 29
 * 00003          30 -- OO
 * </pre>
 */
public class WordCountPartitioner extends Partitioner<Text, IntWritable> {

	private static final int MAXIMUM_LENGTH_SPAN = 30;

	@Override
	public int getPartition(Text key, IntWritable value, int numOfPartitions) {
		if (numOfPartitions == 1) return 0;

		int lengthSpan = Math.max(MAXIMUM_LENGTH_SPAN / (numOfPartitions - 1), 1);
		return Math.min(Math.max(0, (key.toString().length() - 1) / lengthSpan), numOfPartitions - 1);
	}
}
