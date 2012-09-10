package de.tuberlin.schenck.taverna_compilation;

import java.util.Locale;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import de.tuberlin.schenck.taverna_compilation.convert.TavernaToHadoopConverter;
import de.tuberlin.schenck.taverna_compilation.utils.Config;

/**
 * Main class to convert Taverna workflows to Hadoop jobs.
 * Takes care of configuration and initialization.
 * Use --help for help on the command line.
 * 
 * @author schenck
 *
 */
public class TavernaToHadoopMain {
	/** The logger for this class. */
	private static final Logger logger = Logger.getLogger(TavernaToHadoopMain.class);
	/** The version of this program. */
	private static final String VERSION = "0.0.1";
	/** The pattern fo the logger. */
	private static final String LOG_PATTERN = "[%d{yy-MM-dd, HH:mm:ss}|%p|%L: %C] %m%n";
	
	/** The loglevel. */
	private static Level loglevel = Level.INFO;
	
	/** The options for the command line parser. */
	private static Options options;

	/** The input file for the conversion. */
	private static String inputFilename;
	/** The output file for the conversion. */
	private static String outputFilename;
	
	/**
	 * Runs the conversion program.
	 * 
	 * @param args command line arguments
	 */
    public static void main( String[] args ) {
		resetLogger();
    	parseCommandLine(args);
		resetLogger();
		
		Config.readTemplateMapping();
		
    	TavernaToHadoopConverter converter = new TavernaToHadoopConverter(inputFilename, outputFilename);
    	converter.convert();
    	
    	logger.info("Done");
    }

	private static void resetLogger() {
		// Reset logger
		Logger.getRootLogger().getLoggerRepository().resetConfiguration();
    	// Configure logger
		ConsoleAppender console = new ConsoleAppender(); //create appender
		//configure the appender
		console.setLayout(new PatternLayout(LOG_PATTERN)); 
		console.setThreshold(loglevel);
		console.activateOptions();
		//add appender to any Logger (here is root)
		Logger.getRootLogger().addAppender(console);
	}

    /**
     * Definition, parsing and interrogation of the command line arguments. 
     * 
     * @param args the command line arguments
     * @see Options
     * @see Option
     * @see CommandLineParser
     */
	private static void parseCommandLine(String[] args) {
		// Using CLI parser, Posix style
		options = new Options();
		
		// Verbosity option
		OptionBuilder.withArgName("level");
		OptionBuilder.hasArg();
		OptionBuilder.withDescription("verbosity level of logger [debug|info|warn|error|fatal]");
		OptionBuilder.withLongOpt("verbosity");
		Option verbosity = OptionBuilder.create("l");
		options.addOption(verbosity);
		
		// Input option
		OptionBuilder.withArgName("file");
		OptionBuilder.hasArg();
		OptionBuilder.withDescription("taverna workflow as input");
		OptionBuilder.withLongOpt("input");
		Option input = OptionBuilder.create("i");
		options.addOption(input);

		// Output option
		OptionBuilder.withArgName("file");
		OptionBuilder.hasArg();
		OptionBuilder.withDescription("jar file as output");
		OptionBuilder.withLongOpt("output");
		Option output = OptionBuilder.create("o");
		options.addOption(output);

		// Templates path option
		OptionBuilder.withArgName("path");
		OptionBuilder.hasArg();
		OptionBuilder.withDescription("path to template files");
		OptionBuilder.withLongOpt("templates");
		Option templateLocation = OptionBuilder.create("t");
		options.addOption(templateLocation);

		// Templates path option
		OptionBuilder.withArgName("filename");
		OptionBuilder.hasArg();
		OptionBuilder.withDescription("name of mapping file activity->template");
		OptionBuilder.withLongOpt("mappingfile");
		Option templateMappingLocation = OptionBuilder.create("m");
		options.addOption(templateMappingLocation);
		
		// Hadoop class name option
		OptionBuilder.withArgName("name");
		OptionBuilder.hasArg();
		OptionBuilder.withDescription("name of the resulting hadoop class");
		OptionBuilder.withLongOpt("hadoopclassname");
		Option hadoopClassNameOption = OptionBuilder.create("C");
		options.addOption(hadoopClassNameOption);

		// Hadoop package name option
		OptionBuilder.withArgName("name");
		OptionBuilder.hasArg();
		OptionBuilder.withDescription("name of the package for the resulting hadoop class");
		OptionBuilder.withLongOpt("hadooppackagename");
		Option hadoopPackageNameOption = OptionBuilder.create("P");
		options.addOption(hadoopPackageNameOption);
		
		// Help options
		options.addOption("h", "help", false, "print help");
		options.addOption("v", "version", false, "print version");
		
		// Parse given arguments
		CommandLineParser parser = new PosixParser();
		try {
			CommandLine cmd = parser.parse(options, args);
			
			if(cmd.hasOption("h"))
				printHelp();
			
			if(cmd.hasOption("v"))
				printVersion();
			
			// Set loglevel
			String loglevelOption = cmd.getOptionValue("l");
			if(loglevelOption != null) {
				if(loglevelOption.equalsIgnoreCase("debug")) {
					loglevel = Level.DEBUG;
					logger.info("Set loglevel to " + loglevelOption);
				} else if (loglevelOption.equalsIgnoreCase("info")) {
					loglevel = Level.INFO;
					logger.info("Set loglevel to " + loglevelOption);
				} else if (loglevelOption.equalsIgnoreCase("warn")) {
					loglevel =  Level.WARN;
					logger.info("Set loglevel to " + loglevelOption);
				} else if (loglevelOption.equalsIgnoreCase("error")) {
					loglevel = Level.ERROR;
					logger.info("Set loglevel to " + loglevelOption);
				} else if (loglevelOption.equalsIgnoreCase("fatal")) {
					loglevel = Level.FATAL;
					logger.info("Set loglevel to " + loglevelOption);
				} else {
					logger.warn("Could not set loglevel '" + loglevelOption + "'");
				}
			}
			
			// Get and check mandatory arguments
			inputFilename = cmd.getOptionValue("i");
			outputFilename = cmd.getOptionValue("o");
			if(inputFilename == null || outputFilename == null) {
				logger.error("You need to set names for the input and output files.");
				printHelp(1);
			}
			logger.info("Input file: " + inputFilename);
			logger.info("Output file: " + outputFilename);
			
			// Name Hadoop Class if not already done by user
			if(Config.getHadoopClassName().equals("HadoopClass")) {
				String newHadoopClassName = outputFilename.substring(0, outputFilename.lastIndexOf("."));
				
				// Remove all non-word characters
				newHadoopClassName = newHadoopClassName.replaceAll("\\W", "");
				
				// First letter uppercase
				newHadoopClassName = newHadoopClassName.substring(0, 1).toUpperCase(Locale.ENGLISH) + newHadoopClassName.substring(1);
				Config.setHadoopClassName(newHadoopClassName);
			}
			
			// New path to templates?
			String newPathToTemplates = cmd.getOptionValue("t");
			if(newPathToTemplates != null) {
				// Add trailing slash
				if(!newPathToTemplates.endsWith("/"))
					newPathToTemplates += "/";
				
				Config.setPathToTemplates(newPathToTemplates);
				
				logger.info("Path to templates: " + newPathToTemplates);
			}
			
			// New mapping file?
			String newMappingFile = cmd.getOptionValue("m");
			if(newMappingFile != null) {
				Config.setTemplateMappingFile(newMappingFile);
				
				logger.info("Mapping file: " + newMappingFile);
			}
			
			// New hadoop class name?
			String hadoopClassName = cmd.getOptionValue("C");
			if(hadoopClassName != null) {
				Config.setHadoopClassName(hadoopClassName);
			}

			// New hadoop package name?
			String hadoopPackageName = cmd.getOptionValue("P");
			if(hadoopPackageName != null) {
				Config.setHadoopPackageName(hadoopPackageName);
			}
			logger.info("Hadoop Class Name: " + Config.getHadoopClassName());
		} catch (ParseException e) {
			logger.error("Could not parse command line", e);
			printHelp(1);
		}
	}

	/**
	 * Prints the help to the command line and exits with exit status 0.
	 */
	private static void printHelp() {
		printHelp(0);
	}
	
	/**
	 * Prints the help to the command line and exits with the given exit status.
	 * 
	 * @param exitStatus the exit status for <code>System.exit()</code>
	 */
	private static void printHelp(int exitStatus) {
		printVersion(false);
		
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("converter [options]", options);
		
		System.exit(exitStatus);
	}

	/**
	 * Print the version of the program and exit.
	 */
	private static void printVersion() {
		printVersion(true);
	}

	/**
	 * Print the version of the program and exit if desired.
	 * 
	 * @param andExit whether or not to exit after output
	 */
	private static void printVersion(boolean andExit) {
		System.out.println("Taverna to Hadoop compiler version " + VERSION);
		
		if(andExit)
			System.exit(0);
	}
}
