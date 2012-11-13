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
import uk.org.taverna.scufl2.api.port.InputProcessorPort;
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
	 * Converts the workflow to a linear list of MapReduce {@link de.tuberlin.schenck.taverna_to_hadoop.convert.activity_configs.ActivityConfig}.
	 * Starts at output port and recursively goes backwards until input port is reached.
	 * Then reverses the order.
	 */
	private List<ActivityConfig> createListFromWorkflow() {
		logger.info("Converting workflow to list of hadoop jobs.");
		List<ActivityConfig> result = new ArrayList<ActivityConfig>();

		Scufl2Tools scufl2tools = new Scufl2Tools();
		NamedSet<OutputWorkflowPort> workflowOutputPorts = workflowBundle.getMainWorkflow().getOutputPorts();

		logger.debug("Nr. of workflow output ports: " + workflowOutputPorts.size());

		// Get all activities which are linked to the output ports
		// Go through all ports, get all feeding activities
		for(OutputWorkflowPort workflowOutPort : workflowOutputPorts) {
			getPreviousProcessors(result, scufl2tools, workflowOutPort);
		}

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

				// Transfer data from Taverna
				activityConfig.fetchActivitySpecificDataFromTavernaConfig(configuration);

				// Add all ports
				List<String> outputPorts = new ArrayList<String>(processor.getOutputPorts().size());
				List<String> inputPorts = new ArrayList<String>(processor.getInputPorts().size());

				for(OutputProcessorPort outputProcessorPort : processor.getOutputPorts()) {
					outputPorts.add(outputProcessorPort.getName());
					
					// Get mapping from input ports to output ports of previous processors
					for(DataLink dataLinkToNext : scufl2tools.datalinksFrom(outputProcessorPort)) {
						String outname = "";
						if(dataLinkToNext.getSendsTo() instanceof InputProcessorPort) {
							InputProcessorPort inPort = (InputProcessorPort) dataLinkToNext.getSendsTo();
							outname = inPort.getParent().getName() + "_" + dataLinkToNext.getSendsTo().getName();
						} else if(dataLinkToNext.getSendsTo() instanceof OutputWorkflowPort) {
							outname = dataLinkToNext.getSendsTo().getName();
						} else {
							throw new UnsupportedWorkflowException("Unknown port type.");
						}
						
						activityConfig.addToPortMap(outputProcessorPort.getName(), outname);
					}
				}
				for(InputProcessorPort inputProcessorPort : processor.getInputPorts()) {
					inputPorts.add(inputProcessorPort.getName());
				}

				activityConfig.setInputPorts(inputPorts);
				activityConfig.setOutputPorts(outputPorts);
			} catch (InstantiationException e) {
				logger.error("Unsupported workflow.", new UnsupportedWorkflowException());
				logger.error("Could not instanciate class " + className + ".", e);
			} catch (IllegalAccessException e) {
				logger.error("Unsupported workflow.", new UnsupportedWorkflowException());
				logger.error("Could not instanciate class " + className + ".", e);
			} catch (ClassNotFoundException e) {
				logger.error("Unsupported workflow.", new UnsupportedWorkflowException());
				logger.error("Could not instanciate class " + className + ".", e);
			} catch (NoSuchMethodException e) {
				logger.error("Unsupported workflow.", new UnsupportedWorkflowException());
				logger.error("Could not instanciate class " + className + ".", e);
			} catch (SecurityException e) {
				logger.error("Unsupported workflow.", new UnsupportedWorkflowException());
				logger.error("Could not instanciate class " + className + ".", e);
			} catch (IllegalArgumentException e) {
				logger.error("Unsupported workflow.", new UnsupportedWorkflowException());
				logger.error("Could not instanciate class " + className + ".", e);
			} catch (InvocationTargetException e) {
				logger.error("Unsupported workflow.", new UnsupportedWorkflowException());
				logger.error("Could not instanciate class " + className + ".", e);
			} catch (UnsupportedWorkflowException e) {
				logger.error("Could not translate workflow.", e);
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

	/**
	 * Get the String for the template for all map and reduce classes from the workflow.
	 *  
	 * @return the java source code for all map and reduce classes
	 */
	public String getMapReduceClasses() {
		StringBuilder resultBuilder = new StringBuilder();

		for(ActivityConfig activityConfig : activityList) {
			resultBuilder.append(activityConfig.getMapReduce());
			resultBuilder.append("\n");
		}

		String result = resultBuilder.toString();
		logger.debug("Map and reduce classes: " + result);
		return result;
	}

	/**
	 * Get the String for the template for all run methods from the workflow.
	 *  
	 * @return the java source code for all the run methods
	 */
	public String getRuns() {
		StringBuilder resultBuilder = new StringBuilder();

		String inputPath = "";
		String intermediatePath;
		for(ActivityConfig activityConfig : activityList) {
			// TODO what if input from args
			StringBuilder pathBuilder = new StringBuilder();
			pathBuilder.append("\"");
			for(String inputPort : activityConfig.getInputPorts()) {
				pathBuilder.append(activityConfig.getName());
				pathBuilder.append("_");
				pathBuilder.append(inputPort);
				pathBuilder.append(",");
			}
			
			pathBuilder.deleteCharAt(pathBuilder.length() - 1);
			pathBuilder.append("\"");
			inputPath = pathBuilder.toString();
			
			intermediatePath = "\"" + activityConfig.getOutputToNextInput().get(activityConfig.getOutputPorts().get(0)) + "\"";

			activityConfig.setInputPath(inputPath);
			activityConfig.setOutputPath(intermediatePath);

			activityConfig.setInputFormat("TextInputFormat");
			activityConfig.setOutputFormat("TextOutputFormat");

			resultBuilder.append(activityConfig.getRun());
			resultBuilder.append("\n");
		}

		return resultBuilder.toString();
	}
}
