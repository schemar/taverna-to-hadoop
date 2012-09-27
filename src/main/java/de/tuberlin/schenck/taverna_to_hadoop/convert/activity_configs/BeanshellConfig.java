package de.tuberlin.schenck.taverna_to_hadoop.convert.activity_configs;

import java.util.ArrayList;
import java.util.List;

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
		List<String> templates = new ArrayList<String>(2);
		templates.add("identity-map.jtemp");
		templates.add("beanshell-activity-reduce.jtemp");
		
		return removePlaceholdersFromTemplate(templates);
	}

	@Override
	public String getRun() {
		List<String> templates = new ArrayList<String>(1);
		templates.add("beanshell-activity-run.jtemp");
		
		return removePlaceholdersFromTemplate(templates);
	}

	/**
	 * Reads all given templates, concatenates them and processes their activity specific placeholders.
	 * 
	 * @param templateNames a list of all templates that shall be read and processed
	 * @return the template with the activity specific placeholders replaced
	 */
	private String removePlaceholdersFromTemplate(List<String> templateNames) {
		// Read the templates
		StringBuilder resultBuilder = new StringBuilder();
		
		for(String templateName : templateNames) {
			resultBuilder.append(FileUtils.readFileIntoString(Config.getPathToTemplates() + templateName));
			resultBuilder.append("\n");
		}
		
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
			} else if(placeholderStripped.equals("<%=inputFormat%>")) {
				result = result.replace(placeholder, getInputFormat());
			} else if(placeholderStripped.equals("<%=outputFormat%>")) {
				result = result.replace(placeholder, getOutputFormat());
			} else if(placeholderStripped.equals("<%=inputPath%>")) {
				result = result.replace(placeholder, getInputPath());
			} else if(placeholderStripped.equals("<%=outputPath%>")) {
				result = result.replace(placeholder, getOutputPath());
			}
		}
		
		logger.debug("Beanshell template: " + result);
		return result;
	}

	public String getScript() {
		return script;
	}

	public void setScript(String script) {
		this.script = script;
	}
}
