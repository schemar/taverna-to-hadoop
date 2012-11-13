package de.tuberlin.schenck.taverna_to_hadoop.convert.activity_configs;

import java.util.List;
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
	
	/** The input path. */
	private String inputPath;
	
	/** The input format class. */
	private String inputFormat;
	
	/** The output path. */
	private String outputPath;
	
	/** The output format class. */
	private String outputFormat;
	

	/** The input ports. */
	private List<String> inputPorts;
	
	/** The output ports. */
	private List<String> outputPorts;
	
	
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
	 * @return the java source code
	 */
	public abstract String getRun();

	/**
	 * Lets this activity config get the individually required data from the Taverna configuration.
	 * 
	 * @param configuration the Taverna configuration
	 */
	public abstract void fetchActivitySpecificDataFromTavernaConfig(Configuration configuration);
	
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

	/**
	 * @return the inputPath
	 */
	public String getInputPath() {
		return inputPath;
	}

	/**
	 * @param inputPath the inputPath to set
	 */
	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}

	/**
	 * @return the inputFormat
	 */
	public String getInputFormat() {
		return inputFormat;
	}

	/**
	 * @param inputFormat the inputFormat to set
	 */
	public void setInputFormat(String inputFormat) {
		this.inputFormat = inputFormat;
	}

	/**
	 * @return the outputPath
	 */
	public String getOutputPath() {
		return outputPath;
	}

	/**
	 * @param outputPath the outputPath to set
	 */
	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	/**
	 * @return the outputFormat
	 */
	public String getOutputFormat() {
		return outputFormat;
	}

	/**
	 * @param outputFormat the outputFormat to set
	 */
	public void setOutputFormat(String outputFormat) {
		this.outputFormat = outputFormat;
	}

	/**
	 * @return the inputPorts
	 */
	public List<String> getInputPorts() {
		return inputPorts;
	}

	/**
	 * @param inputPort the inputPorts to set
	 */
	public void setInputPorts(List<String> inputPorts) {
		this.inputPorts = inputPorts;
	}

	/**
	 * @param inputPort the inputPort to add
	 */
	public void addInputPort(String inputPort) {
		this.inputPorts.add(inputPort);
	}

	/**
	 * @return the outputPorts
	 */
	public List<String> getOutputPort() {
		return outputPorts;
	}

	/**
	 * @param outputPorts the outputPorts to set
	 */
	public void setOutputPorts(List<String> outputPorts) {
		this.outputPorts = outputPorts;
	}

	/**
	 * @param outputPort the outputPort to add
	 */
	public void addOutputPort(String outputPort) {
		this.outputPorts.add(outputPort);
	}	
}
