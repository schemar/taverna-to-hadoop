package de.tuberlin.schenck.taverna_compilation.convert.translators;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import de.tuberlin.schenck.taverna_compilation.utils.Config;
import de.tuberlin.schenck.taverna_compilation.utils.FileUtils;

/**
 * Translates templates into java source files.
 * 
 * @author schenck
 *
 */
public class TemplateTranslator {
	/** The logger for this class. */
	private static Logger logger = Logger.getLogger(TemplateTranslator.class);
		
	/** The pattern for general placeholders. */
	protected final Pattern placeholderPattern = Pattern.compile("<%(.*?)%>");

	/** The matcher for general placeholders. */
	protected Matcher placeholderMatcher;

	/** The pattern for placeholders in quotes. */
	private Pattern inQuotesPattern = Pattern.compile("\"(.*?)\"");

	/** The matcher for placeholders in quotes. */
	private Matcher inQuotesMatcher;
	
	/** The imports for the java class resulting from the template */
	private Set<String> imports;
	
	/** Whether or not this translates the root element. Only the root element needs to be written to disk. */
	private boolean isRoot = false;
	
	/**
	 * Constructs the translator and sets whether it is the root of a translation.
	 * Only the root needs to be written to disk.
	 * <code>isRoot</code> defaults to false.
	 * 
	 * @param isRoot whether this is a root element
	 */
	public TemplateTranslator(boolean isRoot) {
		this.isRoot = isRoot;
		
		imports = new HashSet<String>(1);
	}
	
	/**
	 * Construct the translator.
	 */
	public TemplateTranslator() {
		imports = new HashSet<String>(1);
	}
	
	/**
	 * Translates a template file into a java source file.
	 * Also translates all included template files recursively.
	 * Returns the java source code as a {@link String}.
	 * 
	 * @param template the reference to the template file, which shall be translated
	 * @return the java source code translated from the referenced template file
	 */
	public String translate(File file) {
		logger.info("Translating " + file);
		String template = FileUtils.readFileIntoString(file);
		return translate(template);
	}
	
	/**
	 * Translates a template into a java source file.
	 * Also translates all included templates recursively.
	 * Returns the java source code as a {@link String}.
	 * 
	 * @param template the template as a <code>String</code>, which shall be translated
	 * @return the java source code translated from the referenced template file
	 */
	public String translate(String template) {
		// Read, translate, write (if root element)
		template = replacePlaceholders(template);
		// Cannot replace imports beforehand because of possible inclusion of additional imports from recursively translated templates
		template = replaceImports(template);
		
		if(isRoot) {
			String fileName = "src/main/java/" + Config.getHadoopPackageName().replaceAll("\\.", "/") + "/" + Config.getHadoopClassName() + ".java";
			FileUtils.writeStringIntoFile(fileName, template);
		}
		
		return template;
	}

	/**
	 * Replaces the placeholders in the template with their respective code or file contents.
	 * See <code>README.md</code> for more information on templates.
	 * 
	 * @param template the template as a <code>String</code>
	 * @return a <code>String</code> with the placeholders replaced
	 */
	protected String replacePlaceholders(String template) {
		placeholderMatcher = placeholderPattern.matcher(template);

		String placeholder;
		while(placeholderMatcher.find()) {
			placeholder = placeholderMatcher.group(0);
			logger.debug("Found placeholder that contains: " + placeholder);
						
			// Find kind of placeholder
			template = handlePlaceholder(template, placeholder);
		}
		
		return template;
	}

	/**
	 * Replaces the placeholder with it's respective value.
	 * 
	 * @param template the template that contains the placeholder
	 * @param placeholder the placeholder what was found
	 * @return the new template with the placeholder replaced
	 */
	protected String handlePlaceholder(String template, String placeholder) {
		// Remove all white spaces to make parsing easier
		String placeholderStripped = placeholder.replaceAll("\\s", "");
		String inQuotes;
		
		if(placeholderStripped.startsWith("<%@includefile")) {
			inQuotesMatcher = inQuotesPattern.matcher(placeholderStripped);
			if(inQuotesMatcher.find()) {
				inQuotes = inQuotesMatcher.group(1);
				logger.debug("Including file: " + inQuotes);
				template = template.replace(placeholder, includeFile(inQuotes, placeholderStripped));
			} else {
				logger.warn("Could not find template from: " + placeholder);
			}
		} else if(placeholderStripped.startsWith("<%@requiresimports")) {
			inQuotesMatcher = inQuotesPattern.matcher(placeholderStripped);
			if(inQuotesMatcher.find()) {
				inQuotes = inQuotesMatcher.group(1);
				logger.debug("Adding includes: " + inQuotes);
				imports.addAll(Arrays.asList(inQuotes.split(",")));
			} else {
				logger.warn("Did not find the imports: " + placeholder);
			}
			
			// Remove placeholder
			template = template.replaceAll(placeholder, "");
		} else if(placeholderStripped.startsWith("<%=")) {
			// Strip opening and closing of tag
			placeholderStripped = placeholderStripped.substring(3, placeholderStripped.length() - 2);
			
			// Which variable to use?
			String replacement = "";
			
			if(placeholderStripped.equalsIgnoreCase("hadoopclassname")) {
				replacement = Config.getHadoopClassName();
			} else if(placeholderStripped.equalsIgnoreCase("hadooppackagename")) {
				replacement = Config.getHadoopPackageName();
			} else {
				logger.error("Could not find variable: " + placeholderStripped);
			}
			
			logger.debug("Replacing '" + placeholder + "' with '" + replacement + "'");
			template = template.replaceAll(placeholder, replacement);
		}
		return template;
	}

	protected String replaceImports(String template) {
		// Move imports into Array to sort them alphabetically
		List<String> importsList = new ArrayList<String>(imports.size());
		importsList.addAll(imports);
		Collections.sort(importsList);
		
		placeholderMatcher = placeholderPattern.matcher(template);
		
		String placeholder;
		String placeholderStripped;
		while(placeholderMatcher.find()) {
			placeholder = placeholderMatcher.group(0);
			logger.debug("Found placeholder that contains: " + placeholder);
			
			// Remove all white spaces to make parsing easier
			placeholderStripped = placeholder.replaceAll("\\s", "");
			if(placeholderStripped.equals("<%@imports%>")) {
				StringBuilder importsStringBuilder = new StringBuilder();
				for(String singleImport : importsList) {
					importsStringBuilder.append("import ");
					importsStringBuilder.append(singleImport);
					importsStringBuilder.append(";\n");
				}
				
				String importsString = importsStringBuilder.toString();
				logger.debug("Adding imports: " + importsString);
				template = template.replaceAll(placeholder, importsString);
			} else {
				logger.warn("Unknown placeholder in template: " + placeholder);
			}
		}
		return template;
	}

	/**
	 * Includes another template inside a template.
	 * 
	 * @param reference the reference to the other template
	 * @param placeholder the placeholder complete and stripped
	 */
	private String includeFile(String reference, String placeholder) {
		TemplateTranslator translator;
		// TODO fix this!
		if(reference.startsWith("beanshell-activity-reduce")) {
			String scriptId = placeholder.split("\\|")[1];
			scriptId = scriptId.substring(0, scriptId.length() - 2);
			translator = new BeanshellTranslator(Config.getActivityConfiguration(scriptId + ""));
		} else {
			translator = new TemplateTranslator();
		}
		
		String result = translator.translate(new File(Config.getPathToTemplates() + reference));
		imports.addAll(translator.getImports());
		
		return result;
	}

	/**
	 * Getter for the imports
	 * 
	 * @return the imports
	 */
	public Set<String> getImports() {
		return imports;
	}
}
