package cz.cvut.bigdata.tfidf.lines;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Receives <b>(key, list['text'])</b> where the key is either a byte offset
 * of the text or a negative number indicating the line number offsets.
 * <p/>
 * First, when the offset are received, the values (converted to int) are
 * summed-up into the line counter. The zero key value indicates the overall
 * number of lines, processed by the mappers. Otherwise, the mapper emits
 * the final result, each time incrementing the line counter.
 */
public class LineNumberReducer extends Reducer<LongWritable, Text, IntWritable, Text> {

	private final IntWritable line = new IntWritable();

	private int lineCounter;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		lineCounter = 1;
	}

	@Override
	protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		if (key.get() < 0L) {
			// setup the line number offset for this reducer
			for (Text text : values) {
				lineCounter += Integer.parseInt(text.toString());
			}
			return;
		} else if (key.get() == 0L) {
			// zero key value indicates the overall number of lines
			int numberOfLines = 0;
			for (Text text : values) {
				numberOfLines += Integer.parseInt(text.toString());
			}
			line.set(0);
			context.write(line, new Text(String.valueOf(numberOfLines)));
			return;
		}

		// emit the result
		line.set(lineCounter);
		context.write(line, values.iterator().next());

		// increment line counter
		lineCounter++;
	}
}
