package de.tuberlin.schenck.taverna_to_hadoop.convert.activity_configs;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import uk.org.taverna.scufl2.api.configurations.Configuration;

/**
 * Configurations interface for Taverna activities.
 * 
 * @author schenck
 *
 */
public abstract class ActivityConfig {
	/** The name of the configuration. */
	private String name;
	
	/** The pattern for general placeholders. */
	protected final Pattern placeholderPattern = Pattern.compile("<%(.*?)%>");

	/** The matcher for general placeholders. */
	protected Matcher placeholderMatcher;

	/** The pattern for placeholders in quotes. */
	protected Pattern inQuotesPattern = Pattern.compile("\"(.*?)\"");

	/** The matcher for placeholders in quotes. */
	protected Matcher inQuotesMatcher;
	
	/**
	 * Constructs an {@link de.tuberlin.schenck.taverna_to_hadoop.convert.activity_configs.ActivityConfig}.
	 * @param name the name of the configuration
	 */
	public ActivityConfig(String name) {
		this.name = name;
	}
	
	/**
	 * The name of the configuration
	 * @return the name
	 */
	public String getName() {
		return name;
	}
	
	/**
	 * The java MapReduce source code for the Hadoop class for this activity.
	 * @return the java source code
	 */
	public abstract String getMapReduce();
	
	/**
	 * The java run method source code for the Hadoop class for this activity.
	 * 
	 * @param inputPath the path to the input file
	 * @param inputFormat the name of the reader class
	 * @param outputPath the path to the output file
	 * @param outputFormat the name of the writer class
	 * @return the java source code
	 */
	public abstract String getRun(String inputPath, String inputFormat, String outputPath, String outputFormat);

	/**
	 * Lets this activity config get the individually required data from the Taverna configuration.
	 * 
	 * @param configuration the Taverna configuration
	 */
	public abstract void fetchDataFromTavernaConfig(Configuration configuration);
	
	@Override
	public String toString() {
		return name;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof ActivityConfig) {
			ActivityConfig otherConfig = (ActivityConfig) obj;
			if(otherConfig.getName().equals(name))
				return true;
		}
		
		return false;
	}
}
