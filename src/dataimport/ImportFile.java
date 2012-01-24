package dataimport;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import dataimport.helpers.CommandLineHelper;
import dataimport.helpers.TableHelper;
import dataimport.mappers.JsonMapper;

public class ImportFile {
	
	private Logger logger;
	private Configuration conf;
	
	private String inputFile;
	private String tableName;
	private String colfam;
	
	public ImportFile() {
		logger = Logger.getLogger(ImportFile.class);
		conf = HBaseConfiguration.create();
	}

	public static void main(String[] args) throws Exception {
		ImportFile importer = new ImportFile();
		importer.setArguments(args);
		importer.createTable();
		Job job = importer.insertData();
		importer.runJob(job);
	}
	
	private void setArguments(String[] args) throws ParseException {
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		CommandLine cmd = CommandLineHelper.parseArgs(otherArgs);
		inputFile = cmd.getOptionValue("i");
		tableName = cmd.getOptionValue("t");
		colfam = cmd.getOptionValue("c");
	}
	
	private void createTable() throws IOException {
		TableHelper th = new TableHelper(conf);
		if (th.existsTable(tableName)) {
			logger.error("Table already exists!");
			System.exit(-1);
		}
		else {
			th.createTable(tableName, colfam);
		}
	}
	
	private Job insertData() throws IOException {
	    Job job = new Job(conf, "Import from file " + inputFile + " into table " + tableName);
	    FileInputFormat.addInputPath(job, new Path(inputFile));
	    job.setJarByClass(ImportFile.class);
	    job.setMapperClass(JsonMapper.class);
	    job.setOutputFormatClass(TableOutputFormat.class);
	    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableName);
	    job.setOutputKeyClass(ImmutableBytesWritable.class);
	    job.setOutputValueClass(Writable.class);
	    job.setNumReduceTasks(0);  
	    return job;
	}
	
	private void runJob(Job job) throws IOException, InterruptedException, ClassNotFoundException {
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
