package cz.cvut.bigdata.tfidf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Implementation of a custom <i>WritableComparable</i> class.
 * It is used by the TF-IDF MapReduce methods for referencing
 * a pair <b>(term, document)</b>, where document is an integer.
 */
public class TermDocWritable implements WritableComparable<TermDocWritable> {

	private String term = null;
	private int document = 0;

	public void set(String term, int document) {
		this.term = term;
		this.document = document;
	}

	public String getTerm() {
		return term;
	}

	public void setTerm(String term) {
		this.term = term;
	}

	public int getDoc() {
		return document;
	}

	public void setDoc(int document) {
		this.document = document;
	}

	public TermDocWritable parse(String text) {
		final String[] parts = StringUtils.split(text, " : ");
		if (parts == null || parts.length < 2) throw new IllegalArgumentException();
		term = parts[0];
		document = Integer.parseInt(parts[1]);
		return this;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(term);
		out.writeInt(document);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		term = in.readUTF();
		document = in.readInt();
	}

	@Override
	public int compareTo(TermDocWritable o) {
		if (o == null) return -1;
		final int val = (term == null) ? 0 : term.compareTo(o.term);
		return (val != 0) ? val : ((document < o.document) ? -1 : ((document == o.document) ? 0 : 1));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof TermDocWritable)) return false;

		final TermDocWritable that = (TermDocWritable) o;
		return document == that.document && ((term != null) ? term.equals(that.term) : that.term == null);
	}

	@Override
	public int hashCode() {
		return 31 * ((term != null) ? term.hashCode() : 0) + document;
	}

	@Override
	public String toString() {
		return String.format("%s : %d", (term != null) ? term : "", document);
	}
}
