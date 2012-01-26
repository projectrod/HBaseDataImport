package dataimport.mappers;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class JSONMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, Writable> {
	
	private int counter = 0;

	/**
	 * Maps the input.
	 * 
	 * @param offset
	 *            The current offset into the input file.
	 * @param line
	 *            The current line of the file.
	 * @param context
	 *            The task context.
	 * @throws java.io.IOException
	 *             When mapping the input fails.
	 */
	@Override
	public void map(LongWritable offset, Text line, Context context)
			throws IOException {
		try {
			JSONParser parser = new JSONParser();
			JSONObject json = (JSONObject) parser.parse(line.toString());
			
			Put put = new Put(Bytes.toBytes(counter));
			for (Object key : json.keySet()) {
				Object val = json.get(key);
				put.add(Bytes.toBytes("value"), Bytes.toBytes(key.toString()),
						Bytes.toBytes(val.toString()));
			}
			context.write(new ImmutableBytesWritable(), put);
			
			counter++;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
