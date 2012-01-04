package mapreduce.dataimport;

import mapreduce.dataimport.mapper.JsonMapper;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ImportFile {

	public static final String NAME = "PrepareJson";

	/**
	 * Main entry point.
	 * 
	 * @param args
	 *            The command line parameters.
	 * @throws Exception
	 *             When running the job fails.
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		CommandLine cmd = parseArgs(otherArgs);

		// get details
		String tableName = cmd.getOptionValue("t");
		String inputFile = cmd.getOptionValue("i");
//		String outputFile = cmd.getOptionValue("o");

		// get table
//		HTable table = new HTable(conf, tableName);

		// create job and set classes etc.
//		Job job = new Job(conf, "Prepare from file " + inputFile + " into "
//				+ tableName);
//		job.setJarByClass(ImportFile.class);
//		job.setMapperClass(JsonMapper.class);
//		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
//		job.setMapOutputValueClass(Writable.class);
//		job.setNumReduceTasks(0);
//		FileInputFormat.setInputPaths(job, new Path(inputFile));
//		FileOutputFormat.setOutputPath(job, new Path(outputFile));
//		HFileOutputFormat.configureIncrementalLoad(job, table);
		
	    Job job = new Job(conf, "Import from file " + inputFile + " into table " + tableName);
	    job.setJarByClass(ImportFile.class);
	    job.setMapperClass(JsonMapper.class);
	    job.setOutputFormatClass(TableOutputFormat.class);
	    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableName);
	    job.setOutputKeyClass(ImmutableBytesWritable.class);
	    job.setOutputValueClass(Writable.class);
	    job.setNumReduceTasks(0);
	    FileInputFormat.addInputPath(job, new Path(inputFile));

		// run the job
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	/**
	 * Parse the command line parameters.
	 * 
	 * @param args
	 *            The parameters to parse.
	 * @return The parsed command line.
	 * @throws org.apache.commons.cli.ParseException
	 *             When the parsing of the parameters fails.
	 */
	private static CommandLine parseArgs(String[] args) throws ParseException {
		// create options
		Options options = new Options();
		Option o = new Option("t", "table", true,
				"table to import into (must exist)");
		o.setRequired(true);
		options.addOption(o);
		o = new Option("i", "input", true,
				"the directory in DFS to read files from");
		o.setRequired(true);
		options.addOption(o);
		o = new Option("o", "output", true, "the file the output is written to");
		o.setRequired(true);
		options.addOption(o);
		
		// check if we are missing parameters
		if (args.length == 0) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(NAME + " ", options, true);
			System.exit(-1);
		}
		
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);
		return cmd;
	}

}
