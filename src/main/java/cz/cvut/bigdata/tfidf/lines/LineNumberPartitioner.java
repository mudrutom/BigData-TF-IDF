package cz.cvut.bigdata.tfidf.lines;

import cz.cvut.bigdata.tfidf.HashPartitioner;
import org.apache.hadoop.io.LongWritable;

/**
 * Extension of the <i>HashPartitioner</i> with special
 * treatment of negative key values, they directly indicate
 * which reducer should receive given key-value pair.
 */
public class LineNumberPartitioner extends HashPartitioner<LongWritable> {

	@Override
	public int getPartition(LongWritable key, Object value, int numPartitions) {
		if (key.get() < 0L) {
			// send a key to a particular reducer
			return Math.abs((int) key.get()) - 1;
		}
		return super.getPartition(key, value, numPartitions);
	}

}
