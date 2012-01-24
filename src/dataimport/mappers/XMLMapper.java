package dataimport.mappers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Iterator;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class XMLMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, Writable> {

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
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	        DocumentBuilder builder = factory.newDocumentBuilder();
	        Document xml = builder.parse(new ByteArrayInputStream(line.getBytes()));
		
	        Node root = xml.getFirstChild();
	        NodeList nodes = root.getChildNodes();
	        
	        for (int i = 0; i < nodes.getLength(); i++) {
	        	Element e = (Element) nodes.item(i);
	        	System.out.println(e.getNodeName());
	        }
//			JSONObject json = (JSONObject) parser.parse(line.toString());
//			String link = (String) json.get("link");
//			byte[] md5Url = DigestUtils.md5(link);
//			Put put = new Put(md5Url);
//			for (Object key : json.keySet()) {
//				Object val = json.get(key);
//				put.add(Bytes.toBytes("data"), Bytes.toBytes(key.toString()),
//						Bytes.toBytes(val.toString()));
//			}
			//context.write(new ImmutableBytesWritable(md5Url), put);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
