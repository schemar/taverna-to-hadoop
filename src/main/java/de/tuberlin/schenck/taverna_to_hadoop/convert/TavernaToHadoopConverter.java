package de.tuberlin.schenck.taverna_to_hadoop.convert;

import org.apache.log4j.Logger;

import de.tuberlin.schenck.taverna_to_hadoop.convert.translators.TemplateTranslator;
import de.tuberlin.schenck.taverna_to_hadoop.exceptions.UnsupportedWorkflowException;
import de.tuberlin.schenck.taverna_to_hadoop.utils.Config;
import de.tuberlin.schenck.taverna_to_hadoop.utils.FileUtils;


/**
 * Handles the actual conversion process.
 * 
 * @author schenck
 *
 */
public class TavernaToHadoopConverter {
	/** The logger for this class. */
	private static Logger logger = Logger.getLogger(TavernaToHadoopConverter.class);
	
	/** Path/file of the workflow that is to be translated. */
	private String inputWorkflow;
	/** Path/file of the resulting Hadoop jar. */
	private String outputHadoop;
	
	/**
	 * Creates the converter.
	 * It needs to know what to read and where to write.
	 * 
	 * @param inputWorkflow the workflow that is to be translated
	 * @param outputHadoop where the resulting jar should be put
	 */
	public TavernaToHadoopConverter(String inputWorkflow, String outputHadoop) {
		this.inputWorkflow = inputWorkflow;
		this.outputHadoop = outputHadoop;
	}

	/**
	 * Convert a workflow to a Hadoop program.
	 * <p>
	 * Reads the workflow first and converts it.
	 * Then uses the appropriate templates to generate Hadoop jobs.
	 */
	public void convert() {
		WorkflowManager workflowManager = null;
		
		try {
			workflowManager = new WorkflowManager(inputWorkflow);
		} catch (UnsupportedWorkflowException e) {
			logger.error("The workflow you tried to convert is not supported.", e);
		}
		
		// Get input template from workflowManager
		String template = FileUtils.readFileIntoString(Config.getPathToTemplates() + "hadoop-wrapper.jtemp");
		template = workflowManager.createTemplateFromWorkflow(template);
		
		// Translate Template
		TemplateTranslator translator = new TemplateTranslator(true);
		template = translator.translate(template);
	}
}
