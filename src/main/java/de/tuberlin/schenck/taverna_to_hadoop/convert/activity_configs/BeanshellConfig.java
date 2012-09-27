package de.tuberlin.schenck.taverna_to_hadoop.convert.activity_configs;

import org.apache.log4j.Logger;

import uk.org.taverna.scufl2.api.configurations.Configuration;
import uk.org.taverna.scufl2.api.property.PropertyException;
import uk.org.taverna.scufl2.translator.t2flow.defaultactivities.BeanshellActivityParser;
import de.tuberlin.schenck.taverna_to_hadoop.utils.Config;
import de.tuberlin.schenck.taverna_to_hadoop.utils.FileUtils;

public class BeanshellConfig extends ActivityConfig {
	/** The logger for this class. */
	private static Logger logger = Logger.getLogger(BeanshellConfig.class);
	
	/** The <code>beanshell</code> script. */
	private String script;
	
	public BeanshellConfig(String name) {
		super(name);
	}

	@Override
	public void fetchDataFromTavernaConfig(Configuration configuration) {
		script = "";
		try {
			script = configuration.getPropertyResource().getPropertyAsString(BeanshellActivityParser.ACTIVITY_URI.resolve("#script"));
		} catch (PropertyException e) {
			logger.error("Could not get script for beanshell.", e);
		}
	}

	@Override
	public String getMapReduce() {
		// Read the templates
		StringBuilder resultBuilder = new StringBuilder();
		resultBuilder.append(FileUtils.readFileIntoString(Config.getPathToTemplates() + "identity-map.jtemp"));
		resultBuilder.append("\n");
		resultBuilder.append(FileUtils.readFileIntoString(Config.getPathToTemplates() + "beanshell-activity-reduce.jtemp"));
		
		String result = resultBuilder.toString();
		// Replace variables
		placeholderMatcher = placeholderPattern.matcher(result);

		String placeholder;
		String placeholderStripped;
		while(placeholderMatcher.find()) {
			placeholder = placeholderMatcher.group(0);
			logger.debug("Found placeholder that contains: " + placeholder);
			
			// Remove all white spaces to make parsing easier
			placeholderStripped = placeholder.replaceAll("\\s", "");
			
			if(placeholderStripped.equals("<%=configName%>")) {
				result = result.replace(placeholder, getName());
			} else if(placeholderStripped.equals("<%=script%>")) {
				result = result.replace(placeholder, "\"" + script + "\"");
			}
		}
		
		logger.debug("Beanshell template: " + result);
		return result;
	}

	@Override
	public String getRun(String inputPath, String inputFormat, String outputPath,
			String outputFormat) {
		// TODO Auto-generated method stub
		return null;
	}

	public String getScript() {
		return script;
	}

	public void setScript(String script) {
		this.script = script;
	}
}
