package dataimport.mappers;

import java.io.IOException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class JsonMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, Writable> {

	private JSONParser parser = new JSONParser();

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
			JSONObject json = (JSONObject) parser.parse(line.toString());
			String link = (String) json.get("link");
			byte[] md5Url = DigestUtils.md5(link);
			Put put = new Put(md5Url);
			for (Object key : json.keySet()) {
				Object val = json.get(key);
				put.add(Bytes.toBytes("data"), Bytes.toBytes(key.toString()),
						Bytes.toBytes(val.toString()));
			}
			context.write(new ImmutableBytesWritable(md5Url), put);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
