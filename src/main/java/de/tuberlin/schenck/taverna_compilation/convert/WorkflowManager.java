package de.tuberlin.schenck.taverna_compilation.convert;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import uk.org.taverna.scufl2.api.container.WorkflowBundle;
import uk.org.taverna.scufl2.api.io.ReaderException;
import uk.org.taverna.scufl2.api.io.WorkflowBundleIO;
import de.tuberlin.schenck.taverna_compilation.convert.translators.WorkflowTranslator;
import de.tuberlin.schenck.taverna_compilation.exceptions.UnsupportedWorkflowException;

public class WorkflowManager {
	/** The logger for this class. */
	private static Logger logger = Logger.getLogger(WorkflowManager.class);
	
	/** The workflow bundle read from disk. */
	private WorkflowBundle workflowBundle;
	
	/**
	 * Create a manager and read the workflow from disk.
	 * 
	 * @param input handle to the workflow (filename or path)
	 * @throws UnsupportedWorkflowException unsupported workflows
	 */
	public WorkflowManager(String input) throws UnsupportedWorkflowException {
		readWorkflow(input);
	}
	
	/**
	 * Reads a Taverna workflow from disk.
	 * Format can be either t2flow or scufl2.
	 * 
	 * @param input the reference to the workflow file
	 * @throws UnsupportedWorkflowException unsupported workflows
	 */
	private void readWorkflow(String input) throws UnsupportedWorkflowException {
		WorkflowBundleIO io = new WorkflowBundleIO();
		File file = new File(input);
		try {
			logger.debug("Reading workflow: " + file);
			// mediaType = null  --> guess
			workflowBundle = io.readBundle(file, null);
		} catch (ReaderException e) {
			logger.error("Could not read " + input, e);
		} catch (IOException e) {
			logger.error("Could not read " + input, e);
		}
		
		checkWorkflowSupport();
	}

	/**
	 * Check whether the read workflow is supported.
	 * <p>
	 * Support depends on the version of this program.
	 * Newer versions naturally support more workflows.
	 * 
	 * @throws UnsupportedWorkflowException the workflow is not supported (at the moment)
	 */
	private void checkWorkflowSupport() throws UnsupportedWorkflowException {
		// Only one workflow supported at the moment
		if(workflowBundle.getWorkflows().size() > 1)
			throw new UnsupportedWorkflowException("Found more than one workflow in the bundle.");
		
		// Only one input port supported at the moment
		if(workflowBundle.getMainWorkflow().getInputPorts().size() != 1)
			throw new UnsupportedWorkflowException("Found more or fewer than one input port.");
		
		// Only one output port is supported at the moment
		if(workflowBundle.getMainWorkflow().getOutputPorts().size() != 1)
			throw new UnsupportedWorkflowException("Found more or fewer than one output port.");
		
		// Only one processor is supported at the moment
		if(workflowBundle.getMainWorkflow().getProcessors().size() != 1)
			throw new UnsupportedWorkflowException("Found more or fewer than one processor.");
		
		// Only beanshell activities are allowed at the moment
		if(!workflowBundle.getMainProfile().getActivities().first().getConfigurableType().toString().equals("http://ns.taverna.org.uk/2010/activity/beanshell"))
			throw new UnsupportedWorkflowException("Found activity other than beanshell: " + workflowBundle.getMainProfile().getActivities().first().getConfigurableType());
		
		// Only one input port for the beanshell allowed at the moment
		if(workflowBundle.getMainProfile().getActivities().first().getInputPorts().size() != 1)
			throw new UnsupportedWorkflowException("Found more or fewer than one input port for activity.");

		// Only one output port for the beanshell allowed at the moment
		if(workflowBundle.getMainProfile().getActivities().first().getOutputPorts().size() != 1)
			throw new UnsupportedWorkflowException("Found more or fewer than one output port for activity.");
	}
	
	/**
	 * Create a template with the correct placeholders for this workflow.
	 * <p>
	 * To create that template, a given standard template gets new placeholders injected.
	 * These placeholders reflect the setup of the workflow.
	 * In order to work, the standard template needs to include the placeholders 
	 * <code><%@ include mapreduce %></code> and <code><%@ include run %></code>.
	 * 
	 * @param inputTemplate the original template
	 * @return the template
	 */
	public String createTemplateFromWorkflow(String inputTemplate) {
		WorkflowTranslator translator = new WorkflowTranslator(workflowBundle);
		return translator.translate(inputTemplate);
	}
}
