package de.tuberlin.schenck.taverna_to_hadoop.convert.activity_configs;
/**
 * Configurations interface for Taverna activities.
 * 
 * @author schenck
 *
 */
public abstract class ActivityConfig {
	private String name;
	
	/**
	 * Constructs an {@link de.tuberlin.schenck.taverna_to_hadoop.convert.activity_configs.ActivityConfig}.
	 * @param name the name of the configuration
	 */
	public ActivityConfig(String name) {
		this.name = name;
	}
	
	/**
	 * The name of the configuration
	 * @return the name
	 */
	public String getName() {
		return name;
	}
	
	/**
	 * The java MapReduce source code for the Hadoop class for this activity.
	 * @return the java source code
	 */
	public abstract String getMapReduce();
	
	/**
	 * The java run method source code for the Hadoop class for this activity.
	 * @return the java source code
	 */
	public abstract String getRun();

	@Override
	public String toString() {
		return name;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof ActivityConfig) {
			ActivityConfig otherConfig = (ActivityConfig) obj;
			if(otherConfig.getName().equals(name))
				return true;
		}
		
		return false;
	}
}
