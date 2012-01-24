package dataimport.helpers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class TableHelper {

	private HBaseAdmin admin = null;

	public TableHelper(Configuration conf) throws IOException {
		this.admin = new HBaseAdmin(conf);
	}

	public static TableHelper getHelper(Configuration conf) throws IOException {
		return new TableHelper(conf);
	}

	public boolean existsTable(String table) throws IOException {
		return admin.tableExists(table);
	}

	public void createTable(String table, String... colfams)
			throws IOException {
		HTableDescriptor desc = new HTableDescriptor(table);
		for (String cf : colfams) {
			HColumnDescriptor coldef = new HColumnDescriptor(cf);
			desc.addFamily(coldef);
		}
		admin.createTable(desc);
	}

	public void disableTable(String table) throws IOException {
		admin.disableTable(table);
	}

	public void dropTable(String table) throws IOException {
		if (existsTable(table)) {
			disableTable(table);
			admin.deleteTable(table);
		}
	}

}
