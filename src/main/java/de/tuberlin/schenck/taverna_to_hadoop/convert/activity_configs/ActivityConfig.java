package de.tuberlin.schenck.taverna_to_hadoop.convert.activity_configs;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
	
	// TODO not static, but depending on activity (IF necessary)
	/** The output key class. */
	private String outputKeyClass = "Text.class";
	
	/** The output value class. */
	private String outputValueClass = "Text.class";
	

	/** The input ports and their originating activity. */
	private Map<String, String> inputPorts;
	
	/** The output ports. */
	private List<String> outputPorts;
	
	
	/** A map that maps input ports to the output ports of the previous processor. */
	private Map<String, String> outputToNextInput;
	
	
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
		outputToNextInput = new HashMap<String, String>();
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

	public String getMultipleOutputsWrite() {
		StringBuilder resultBuilder = new StringBuilder();
		
		for(String outputPort : outputPorts) {
			resultBuilder.append("newValue.set(interpreter.get(\"");
			resultBuilder.append(outputPort);			
			resultBuilder.append("\").toString());\n");
			resultBuilder.append("\t\t\t\t");
			resultBuilder.append("mos.write(\"");
			resultBuilder.append(outputToNextInput.get(outputPort));
			resultBuilder.append("\", key, newValue);\n");
			resultBuilder.append("\t\t\t\t");
		}
		
		return resultBuilder.toString();
	}
	
	public String getMultipleOutputsRun() {
		StringBuilder resultBuilder = new StringBuilder();

		for(String outputPort : outputPorts) {
			resultBuilder.append("MultipleOutputs.addNamedOutput(jobConf");
			resultBuilder.append(name);			
			resultBuilder.append(", \"");
			resultBuilder.append(outputToNextInput.get(outputPort));
			resultBuilder.append("\", TextOutputFormat.class, ");
			resultBuilder.append(outputKeyClass);
			resultBuilder.append(", ");
			resultBuilder.append(outputValueClass);
			resultBuilder.append(");\n");
			resultBuilder.append("\t\t");
		}
		
		return resultBuilder.toString();
	}
	
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
	public Map<String, String> getInputPorts() {
		return inputPorts;
	}

	/**
	 * @param inputPort the inputPorts to set
	 */
	public void setInputPorts(Map<String, String> inputPorts) {
		this.inputPorts = inputPorts;
	}

	/**
	 * @param inputPort the inputPort to add
	 */
	public void addInputPort(String inputPort, String originatingActivity) {
		this.inputPorts.put(inputPort, originatingActivity);
	}

	/**
	 * @return the outputPorts
	 */
	public List<String> getOutputPorts() {
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
	
	/**
	 * @param thisOutputPort the output port of this activity
	 * @param nextInputPort the input port of the activity that gets the output from this output port
	 */
	public void addToPortMap(String thisOutputPort, String nextInputPort) {
		outputToNextInput.put(thisOutputPort, nextInputPort);
	}

	/**
	 * @return the outputToNextInput
	 */
	public Map<String, String> getOutputToNextInput() {
		return outputToNextInput;
	}

	/**
	 * @return the outputKeyClass
	 */
	public String getOutputKeyClass() {
		return outputKeyClass;
	}

	/**
	 * @param outputKeyClass the outputKeyClass to set
	 */
	public void setOutputKeyClass(String outputKeyClass) {
		this.outputKeyClass = outputKeyClass;
	}

	/**
	 * @return the outputValueClass
	 */
	public String getOutputValueClass() {
		return outputValueClass;
	}

	/**
	 * @param outputValueClass the outputValueClass to set
	 */
	public void setOutputValueClass(String outputValueClass) {
		this.outputValueClass = outputValueClass;
	}
}
