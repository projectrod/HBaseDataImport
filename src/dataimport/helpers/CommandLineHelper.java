package dataimport.helpers;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class CommandLineHelper {

	public static CommandLine parseArgs(String[] args) throws ParseException {
		// create options
		Options options = new Options();
		Option i = new Option("i", "input", true,
				"the directory in DFS to read files from");
		i.setRequired(true);
		options.addOption(i);
		Option t = new Option("t", "table", true,
				"table to import into");
		t.setRequired(true);
		options.addOption(t);
		Option p = new Option("p", "parser type", true,
				"type parser to use, for example xml or json");
		p.setRequired(true);
		options.addOption(p);
		
		// check if we are missing parameters
		if (args.length == 0) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("Options ", options, true);
			System.exit(-1);
		}
		
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);
		return cmd;
	}
	
}
