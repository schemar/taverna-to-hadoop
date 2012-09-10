package de.tuberlin.schenck.taverna_compilation.convert.translators;

import org.apache.log4j.Logger;

public class BeanshellTranslator extends TemplateTranslator {
	/** The logger for this class. */
	private static Logger logger = Logger.getLogger(BeanshellTranslator.class);
	
	/** The Beanshell script. */
	private String script;
	
	public BeanshellTranslator(String script) {
		super();
		
		this.script = script;
	}
	
	@Override
	public String translate(String template) {		
		// translate
		template = replaceActivitySpecificPlaceholders(template);
		template = replacePlaceholders(template);
		
		return template;
	}

	/**
	 * Replaces the placeholder that are beanshell specific.
	 * 
	 * @param template the template that contains the placeholders
	 * @return the template with the placeholders replaced with values
	 */
	private String replaceActivitySpecificPlaceholders(String template) {
		placeholderMatcher = placeholderPattern.matcher(template);

		String placeholder;
		while(placeholderMatcher.find()) {
			placeholder = placeholderMatcher.group(0);
			logger.debug("Found placeholder that contains: " + placeholder);
						
			// Remove all white spaces to make parsing easier
			String placeholderStripped = placeholder.replaceAll("\\s", "");
			
			if(placeholderStripped.startsWith("<%=script")) {
				script = script.replace("\"", "\\\"");
				logger.debug("Replacing '" + placeholder + "' with '" + script + "'");
				template = template.replace(placeholder, "\"" + script + "\"");
			}
		}
		
		return template;
	}

}
