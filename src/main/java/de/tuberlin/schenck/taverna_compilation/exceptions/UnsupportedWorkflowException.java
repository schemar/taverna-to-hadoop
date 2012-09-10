package de.tuberlin.schenck.taverna_compilation.exceptions;

public class UnsupportedWorkflowException extends Exception {
	private static final long serialVersionUID = 4651018486411301562L;
	
	/**
	 * Constructs an <code>UnsupportedWorkflowException</code>.
	 */
	public UnsupportedWorkflowException() {
		super();
	}
	
	/**
	 * Constructs an <code>UnsupportedWorkflowException</code> with the specified detail message.
	 * 
	 * @param message the detail message
	 */
	public UnsupportedWorkflowException(String message) {
		super(message);
	}
}
