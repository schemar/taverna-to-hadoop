package de.tuberlin.schenck.taverna_compilation.convert.translators;

import org.apache.log4j.Logger;

import uk.org.taverna.scufl2.api.activity.Activity;
import uk.org.taverna.scufl2.api.container.WorkflowBundle;
import uk.org.taverna.scufl2.api.property.PropertyException;
import uk.org.taverna.scufl2.translator.t2flow.defaultactivities.BeanshellActivityParser;
import de.tuberlin.schenck.taverna_compilation.utils.Config;

public class WorkflowTranslator extends TemplateTranslator {
	/** The logger for this class. */
	private static Logger logger = Logger.getLogger(WorkflowTranslator.class);
	
	private WorkflowBundle workflowBundle;
	
	/**
	 * Translates the wrapper template into a template that contains Map and Reduce calls.
	 * The wrapper template only specifies the positions of Map, Reduce, and Run.
	 * This translator will put the includes of the correct individual templates at those positions.
	 * <p>
	 * The workflow dictates the number and sequence of the Mappers and Reducers.
	 * The according information is read from the {@link uk.org.taverna.scufl2.api.container.WorkflowBundle}.
	 * 
	 * @param workflowBundle
	 */
	public WorkflowTranslator(WorkflowBundle workflowBundle) {
		super();
		this.workflowBundle = workflowBundle;
	}
	
	@Override
	public String translate(String template) {
		// Read, translate, write (if root element)
		template = replacePlaceholders(template);
		
		return template;
	}

	@Override
	protected String handlePlaceholder(String template, String placeholder) {
		// Remove all white spaces to make parsing easier
		String placeholderStripped = placeholder.replaceAll("\\s", "");
		
		Activity activity = workflowBundle.getMainProfile().getActivities().first();
		
		String replacement;
		if(placeholderStripped.equalsIgnoreCase("<%@includemapreduce%>")) {
			// Beanshell
			if(activity.getConfigurableType().toString().equals("http://ns.taverna.org.uk/2010/activity/beanshell")) {
				if(Config.getMapperMapping().get("http://ns.taverna.org.uk/2010/activity/beanshell").length() > 0) {
					int configNumber = Config.getCount();
					
					replacement = "<%@ include file = \"" + Config.getMapperMapping().get("http://ns.taverna.org.uk/2010/activity/beanshell") + "\"" +
							" | " + configNumber +
							" %>";
					
					// TODO Cannot stay as "first"
					try {
						Config.putActivityConfiguration(configNumber + "", activity.getParent().getConfigurations().first().getPropertyResource().getPropertyAsString(BeanshellActivityParser.ACTIVITY_URI.resolve("#script")));
					} catch (PropertyException e) {
						logger.error("Could not get Script for Beanshell.", e);
					}
				} else {
					replacement = "<%@ include file = \"identity-map.jtemp\" %>";
				}
				
				if(Config.getReducerMapping().get("http://ns.taverna.org.uk/2010/activity/beanshell") != "") {
					int configNumber = Config.getCount();
					
					replacement += "\n<%@ include file = \"" + Config.getReducerMapping().get("http://ns.taverna.org.uk/2010/activity/beanshell") + "\"" +
							" | " + configNumber +
							" %>";
					
					// TODO Cannot stay as "first"
					try {
						Config.putActivityConfiguration(configNumber + "", activity.getParent().getConfigurations().first().getPropertyResource().getPropertyAsString(BeanshellActivityParser.ACTIVITY_URI.resolve("#script")));
					} catch (PropertyException e) {
						logger.error("Could not get Script for Beanshell.", e);
					}
				} else {
					replacement += "\n<%@ include file = \"identity-reduce.jtemp\" %>";
				}
				
				logger.debug("Replacing placeholder with: " + replacement);
				template = template.replaceAll(placeholder, replacement);
			}
		} else if(placeholderStripped.equalsIgnoreCase("<%@includerun%>")) {
			// TODO Depending on activity
			replacement = "<%@ include file = \"beanshell-activity-run.jtemp\" %>";

			logger.debug("Replacing placeholder with: " + replacement);
			template = template.replaceAll(placeholder, replacement);
		}
		return template;
	}
	
}
