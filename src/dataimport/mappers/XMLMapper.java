package dataimport.mappers;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
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
	        
	        for (int indexElement = 0; indexElement < 10; indexElement++) {
	        	Element element = (Element) nodes.item(indexElement);
				Put put = new Put(Bytes.toBytes(indexElement));

				if (element.hasAttributes()) {
					NamedNodeMap attributes = element.getAttributes();
					for (int indexAttribute = 0; indexAttribute < attributes
							.getLength(); indexAttribute++) {
						Attr attribuut = (Attr) attributes.item(indexAttribute);
						put.add(Bytes.toBytes("attributes"),
								Bytes.toBytes(attribuut.getNodeName()),
								Bytes.toBytes(attribuut.getNodeValue()));
					}
				}
				if (element.getNodeValue() != null) {
					put.add(Bytes.toBytes("value"), Bytes.toBytes(element.getNodeName()), Bytes.toBytes(element.getNodeValue()));
				}
	        	context.write(new ImmutableBytesWritable() , put);
	        }
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
