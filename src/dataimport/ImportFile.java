package dataimport;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import dataimport.helpers.CommandLineHelper;
import dataimport.helpers.TableHelper;
import dataimport.mappers.JSONMapper;
import dataimport.mappers.XMLMapper;

public class ImportFile {
	
	private Logger logger;
	private Configuration conf;
	
	private String inputFile;
	private String tableName;
	private String parserType;
	
	private String[] supportedParserTypes = new String[] {"xml", "json"};
	
	public ImportFile() {
		logger = Logger.getLogger(ImportFile.class);
		conf = HBaseConfiguration.create();
	}

	public static void main(String[] args) throws Exception {
		ImportFile importer = new ImportFile();
		try {			
			importer.setArguments(args);			
			importer.createTable();
			Job job = importer.insertData();
			importer.runJob(job);
		} catch (ParseException p) {
			throw p;
		} catch (Exception e) {
			importer.dropTable();
			throw e;
		}
	}
	
	private void setArguments(String[] args) throws ParseException {
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		CommandLine cmd = CommandLineHelper.parseArgs(otherArgs);
		inputFile = cmd.getOptionValue("i");
		tableName = cmd.getOptionValue("t");
		if (Arrays.asList(supportedParserTypes).contains(cmd.getOptionValue("p"))) {
			parserType = cmd.getOptionValue("p");
		}
		else {
			throw new ParseException("Not a valid parser type");
		}
	}
	
	private void createTable() throws IOException {
		TableHelper th = new TableHelper(conf);
		if (th.existsTable(tableName)) {
			logger.error("Table already exists!");
			System.exit(-1);
		}
		else {
			th.createTable(tableName, "attributes", "value");
		}
	}
	
	private void dropTable() throws IOException {
		TableHelper th = new TableHelper(conf);
		if (th.existsTable(tableName)) {
			th.dropTable(tableName);
		}
	}
	
	private Job insertData() throws IOException {
		conf.set("mapred.task.timeout", "1800000");
	    Job job = new Job(conf, "Import from file " + inputFile + " into table " + tableName);
	    FileInputFormat.addInputPath(job, new Path(inputFile));
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setJarByClass(ImportFile.class);
	    job.setMapperClass((Class<? extends Mapper>) getMapper().getClass());
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
	
	private Object getMapper() {
		if (parserType.equals("xml")) {
			return new XMLMapper();
		}
		else if (parserType.equals("json")) {
			return new JSONMapper();
		}
		return null;
	}

}
