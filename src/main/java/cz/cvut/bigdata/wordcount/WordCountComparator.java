package cz.cvut.bigdata.wordcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Sorts word keys in reversed lexicographical order.
 */
public class WordCountComparator extends WritableComparator {

	protected WordCountComparator() {
		super(Text.class, true);
	}

	@Override
	@SuppressWarnings("unchecked")
	public int compare(WritableComparable a, WritableComparable b) {
		// Here we exploit the implementation of compareTo(...) in Text.class.
		return -a.compareTo(b);
	}
}
