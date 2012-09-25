package de.tuberlin.schenck.taverna_to_hadoop.convert;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import uk.org.taverna.scufl2.api.container.WorkflowBundle;
import uk.org.taverna.scufl2.api.core.DataLink;
import uk.org.taverna.scufl2.api.io.ReaderException;
import uk.org.taverna.scufl2.api.io.WorkflowBundleIO;
import uk.org.taverna.scufl2.api.port.OutputWorkflowPort;
import uk.org.taverna.scufl2.api.profiles.ProcessorBinding;
import uk.org.taverna.scufl2.api.profiles.ProcessorInputPortBinding;
import uk.org.taverna.scufl2.api.profiles.ProcessorOutputPortBinding;
import de.tuberlin.schenck.taverna_to_hadoop.convert.activity_configs.IConfig;
import de.tuberlin.schenck.taverna_to_hadoop.exceptions.UnsupportedWorkflowException;
import de.tuberlin.schenck.taverna_to_hadoop.utils.Config;

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
		List<IConfig> activityList = createListFromWorkflow();
		return translate(inputTemplate, activityList);
	}
	
	/**
	 * Converts the workflow to a linear list of MapReduce {@link de.tuberlin.schenck.taverna_to_hadoop.convert.activity_configs.IConfig}.
	 * Starts at output port and recursively goes backwards until input port is reached.
	 * Then reverses the order.
	 */
	private List<IConfig> createListFromWorkflow() {
		List<IConfig> result = new ArrayList<IConfig>();
		for(DataLink dataLink : workflowBundle.getMainWorkflow().getDataLinks()) {
			logger.debug(dataLink.getReceivesFrom() + "->" + dataLink.getSendsTo());
		}
		for(ProcessorBinding binding : workflowBundle.getMainProfile().getProcessorBindings()) {
			logger.debug(binding.getName());
			for(ProcessorInputPortBinding inBinding : binding.getInputPortBindings()) {
				logger.debug("\tinput: " + inBinding.getBoundActivityPort() + ", " + inBinding.getBoundProcessorPort());
			}
			for(ProcessorOutputPortBinding outBinding : binding.getOutputPortBindings()) {
				logger.debug("\toutput: " + outBinding);
			}
		}
		for(OutputWorkflowPort port : workflowBundle.getMainWorkflow().getOutputPorts()) {
			
		}
		return result;
	}
	
	/**
	 * Replaces placeholders contained in a template with their respective template imports.
	 * 
	 * @param template the template where to put the map and reduce calls
	 * @param configurations a list of configurations for MapReduce calls
	 * @return the template with the placeholder replaced by file inclusions
	 */
	public String translate(String template, List<IConfig> configurations) {
				
		// TODO put map reduce classes into template
		
		return template;
	}
}
