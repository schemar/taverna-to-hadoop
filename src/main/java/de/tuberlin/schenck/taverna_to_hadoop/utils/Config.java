package de.tuberlin.schenck.taverna_to_hadoop.utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class Config {
	/** The logger for this class. */
	private static Logger logger = Logger.getLogger(Config.class);
	
	/** The default path to the templates. */
	private static String pathToTemplates = "resources/templates/";

	/** The default mapping file from activities to templates. */
	private static String templateMappingFile = "mapping";
	
	/** The default class name of the resulting hadoop class. */
	private static String hadoopClassName = "HadoopClass";
	
	/** The default package name for the resulting hadoop class. */
	private static String hadoopPackageName = "de.tuberlin.schenck.taverna_compilation.generated";
	
	/** A counter that increases every time it is called */
	private static int counter = 0;
	
	/** A map from activity URIs to templates. */
	private static Map<String, String> mapperMapping = new HashMap<String, String>();
	/** A map from activity URIs to templates. */
	private static Map<String, String> reducerMapping = new HashMap<String, String>();
	
	/** All Activity Configurations. */
	private  static Map<String, String> activityConfigurations = new HashMap<String, String>();
	
	public static String getActivityConfiguration(String key) {
		return activityConfigurations.get(key);
	}
	
	public static void putActivityConfiguration(String key, String value) {
		Config.activityConfigurations.put(key, value);
	}
	
	public static String getPathToTemplates() {
		return pathToTemplates;
	}

	public static void setPathToTemplates(String pathToTemplates) {
		Config.pathToTemplates = pathToTemplates;
	}

	public static String getHadoopClassName() {
		return hadoopClassName;
	}

	public static void setHadoopClassName(String hadoopClass) {
		Config.hadoopClassName = hadoopClass;
	}

	public static String getHadoopPackageName() {
		return hadoopPackageName;
	}

	public static void setHadoopPackageName(String hadoopPackageName) {
		Config.hadoopPackageName = hadoopPackageName;
	}
	
	public static String getTemplateMappingFile() {
		return templateMappingFile;
	}

	public static void setTemplateMappingFile(String templateMappingFile) {
		Config.templateMappingFile = templateMappingFile;
	}

	public static int getCount() {
		return Config.counter++;
	}

	public static Map<String, String> getMapperMapping() {
		return mapperMapping;
	}

	public static void setMapperMapping(Map<String, String> mapperMapping) {
		Config.mapperMapping = mapperMapping;
	}

	public static Map<String, String> getReducerMapping() {
		return reducerMapping;
	}

	public static void setReducerMapping(Map<String, String> reducerMapping) {
		Config.reducerMapping = reducerMapping;
	}

	public static void readTemplateMapping() {
		logger.info("Reading file into mapping: " + templateMappingFile);
				
		BufferedReader reader = null;
		
		try {
			reader = new BufferedReader(new FileReader(pathToTemplates + templateMappingFile));
			
			String line;
			while((line = reader.readLine()) != null) {
				String[] splits = line.split("\t");
				mapperMapping.put(splits[0], splits[1]);
				reducerMapping.put(splits[0], splits[2]);
				
				logger.debug("Read line: " + line);
			}
		} catch (FileNotFoundException e) {
			logger.error("Could not read file " + templateMappingFile, e);
		} catch (IOException e) {
			logger.error("Could not read file " + templateMappingFile, e);
		} finally {
			try { reader.close(); } catch (Exception e) { /* ignore */ }
		}
	}
}
