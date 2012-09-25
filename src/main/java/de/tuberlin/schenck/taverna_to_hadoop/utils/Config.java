package de.tuberlin.schenck.taverna_to_hadoop.utils;

import java.util.HashMap;
import java.util.Map;

public class Config {	
	/** The default path to the templates. */
	private static String pathToTemplates = "resources/templates/";
	
	/** The default class name of the resulting hadoop class. */
	private static String hadoopClassName = "HadoopClass";
	
	/** The default package name for the resulting hadoop class. */
	private static String hadoopPackageName = "de.tuberlin.schenck.taverna_to_hadoop.generated";
	
	/** The default package name for activity configs. */
	private static String activityConfigsPackage = "de.tuberlin.schenck.taverna_to_hadoop.convert.activity_configs.";
	
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
	
	public static String getActivityConfigsPackage() {
		return activityConfigsPackage;
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
}
