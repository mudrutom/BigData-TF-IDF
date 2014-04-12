package cz.cvut.bigdata.tfidf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Extension of the <i>TermDocWritable</i> that
 * additionally stores the termDoc frequency.
 */
public class TermDocFreqWritable extends TermDocWritable {

	private int frequency = 0;

	public TermDocFreqWritable() {
	}

	public TermDocFreqWritable(TermDocFreqWritable termDocFreq) {
		setTerm(termDocFreq.getTerm());
		setDoc(termDocFreq.getDoc());
		frequency = termDocFreq.getFreq();
	}

	public void copy(TermDocWritable termDoc) {
		setTerm(termDoc.getTerm());
		setDoc(termDoc.getDoc());
		frequency = 0;
	}

	public void set(String term, int document, int frequency) {
		set(term, document);
		this.frequency = frequency;
	}

	public int getFreq() {
		return frequency;
	}

	public void setFreq(int frequency) {
		this.frequency = frequency;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(frequency);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		frequency = in.readInt();
	}
}
