package cz.cvut.bigdata.tfidf.docs;

import cz.cvut.bigdata.tfidf.HashPartitioner;
import org.apache.hadoop.io.Text;

/**
 * Extension of the <i>HashPartitioner</i> with special
 * treatment of key values starting with '_', they directly
 * indicate which reducer should receive given key-value pair.
 */
public class InverseDocFrequencyPartitioner extends HashPartitioner<Text> {

	@Override
	public int getPartition(Text key, Object value, int numPartitions) {
		final String text = key.toString();
		if (text.startsWith("_")) {
			// send a key to a particular reducer
			return Integer.parseInt(text.substring(1));
		}
		return super.getPartition(key, value, numPartitions);
	}

}
