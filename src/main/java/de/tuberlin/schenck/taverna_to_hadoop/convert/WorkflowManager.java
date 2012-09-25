package de.tuberlin.schenck.taverna_to_hadoop.convert;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import uk.org.taverna.scufl2.api.common.NamedSet;
import uk.org.taverna.scufl2.api.common.Scufl2Tools;
import uk.org.taverna.scufl2.api.configurations.Configuration;
import uk.org.taverna.scufl2.api.container.WorkflowBundle;
import uk.org.taverna.scufl2.api.core.DataLink;
import uk.org.taverna.scufl2.api.core.Processor;
import uk.org.taverna.scufl2.api.io.ReaderException;
import uk.org.taverna.scufl2.api.io.WorkflowBundleIO;
import uk.org.taverna.scufl2.api.port.InputWorkflowPort;
import uk.org.taverna.scufl2.api.port.OutputProcessorPort;
import uk.org.taverna.scufl2.api.port.OutputWorkflowPort;
import uk.org.taverna.scufl2.api.port.ReceiverPort;
import uk.org.taverna.scufl2.api.port.SenderPort;
import de.tuberlin.schenck.taverna_to_hadoop.convert.activity_configs.ActivityConfig;
import de.tuberlin.schenck.taverna_to_hadoop.exceptions.UnsupportedWorkflowException;
import de.tuberlin.schenck.taverna_to_hadoop.utils.Config;

public class WorkflowManager {
	/** The logger for this class. */
	private static Logger logger = Logger.getLogger(WorkflowManager.class);
	
	/** The workflow bundle read from disk. */
	private WorkflowBundle workflowBundle;
	
	/** The activity list created from the workflow for the MapReduce class. */
	List<ActivityConfig> activityList;
	
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
		
		activityList = createListFromWorkflow();
		logger.info("List from workflow: " + activityList);
	}
	
	/**
	 * Converts the workflow to a linear list of MapReduce {@link de.tuberlin.schenck.taverna_to_hadoop.convert.activity_configs.IConfig}.
	 * Starts at output port and recursively goes backwards until input port is reached.
	 * Then reverses the order.
	 */
	private List<ActivityConfig> createListFromWorkflow() {
		logger.info("Converting workflow to list of hadoop jobs.");
		List<ActivityConfig> result = new ArrayList<ActivityConfig>();
		
		Scufl2Tools scufl2tools = new Scufl2Tools();
		NamedSet<OutputWorkflowPort> workflowOutputPorts = workflowBundle.getMainWorkflow().getOutputPorts();
		if(workflowOutputPorts.size() != 1) {
			String message = "Only one output port for workflow allowed at the moment";
			logger.error(message, new UnsupportedWorkflowException(message));
		}
		logger.debug("Nr. of workflow output ports: " + workflowOutputPorts.size());
		
		// Get all activities which are linked to the output ports
		// Go through all ports, get all feeding activities
		for(OutputWorkflowPort workflowOutPort : workflowOutputPorts) {
			getPreviousProcessors(result, scufl2tools, workflowOutPort);
		}
		scufl2tools.datalinksTo(workflowOutputPorts.first());
		
		// Reverse, because we went through the workflow backwards
		Collections.reverse(result);
		return result;
	}

	/**
	 * Get all previous processors connected to the given {@link uk.org.taverna.scufl2.api.port.ReceiverPort} by a {@link uk.org.taverna.scufl2.api.core.DataLink}.
	 * Adds all processors to the end of the list of {@link de.tuberlin.schenck.taverna_to_hadoop.convert.activity_configs.ActivityConfig}s.
	 * If it is already in the list, it deletes it and appends it.
	 * 
	 * @param result the final list of all {@link de.tuberlin.schenck.taverna_to_hadoop.convert.activity_configs.ActivityConfig}s.
	 * @param scufl2tools {@link uk.org.taverna.scufl2.api.common.Scufl2Tools} from the scufl 2 API
	 * @param receiverPort the receiving port
	 */
	private void getPreviousProcessors(List<ActivityConfig> result,
			Scufl2Tools scufl2tools, ReceiverPort receiverPort) {
		// Remember which one to go through next
		Set<ReceiverPort> nextReceiverPorts = new HashSet<ReceiverPort>();
		
		// Go through all links
		for(DataLink dataLink : scufl2tools.datalinksTo(receiverPort)) {
			SenderPort senderPort = dataLink.getReceivesFrom();
			// No processor if input port of workflow
			if(senderPort instanceof InputWorkflowPort)
				continue;
			
			OutputProcessorPort processorOutPort = (OutputProcessorPort) senderPort;
			Processor processor = processorOutPort.getParent();
			Configuration configuration = scufl2tools.configurationForActivityBoundToProcessor(processor, workflowBundle.getMainProfile());
			
			logger.debug("Activity configuration found: "+ configuration.getName() + " - " + configuration.getConfigurableType());
			String[] activityParams = configuration.getConfigurableType().getPath().split("/");
			
			// Find classname for configuration
			String className = activityParams[activityParams.length - 1];
			// First letter uppercase
			className = className.substring(0, 1).toUpperCase() + className.substring(1);
			// Append "Config"
			className += "Config";
			
			ActivityConfig activityConfig = null;
			try {
				logger.debug("Instanciating class: " + className);
				
				// Load class from name
				Class<?> classForName = Class.forName(Config.getActivityConfigsPackage() + className);
				Constructor<?> constructor = classForName.getConstructor(String.class);
				activityConfig = (ActivityConfig) constructor.newInstance(configuration.getName());
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | 
					NoSuchMethodException | SecurityException | IllegalArgumentException | 
					InvocationTargetException e) {
				logger.error("Could not instanciate class " + className + ".", e);
			}
			
			List<ActivityConfig> copyOfResult = new ArrayList<ActivityConfig>(result.size());
			copyOfResult.addAll(result);
			// If it is already in the list, remove and append
			for(int i = 0; i < copyOfResult.size(); i++) {
				if(copyOfResult.get(i).equals(activityConfig)) {
					result.remove(i);
				}
			}
			result.add(activityConfig);
			
			nextReceiverPorts.addAll(processor.getInputPorts());
		}
		
		// Next step in the workflow to reach all
		for(ReceiverPort nextReceiverPort : nextReceiverPorts) {
			getPreviousProcessors(result, scufl2tools, nextReceiverPort);
		}
	}
	
	public String getMapReduceClasses() {
		// FIXME get classes
		return activityList.toString();
	}
	
	public String getRuns() {
		logger.info("Resulting list: " + activityList);
		// FIXME get classes
		return activityList.toString();
	}
}
