package de.tuberlin.schenck.taverna_to_hadoop.utils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import org.apache.log4j.Logger;

/**
 * Utilities to read, write and manipulate files.
 * 
 * @author schenck
 *
 */
public class FileUtils {
	/** The logger for this class. */
	private static Logger logger = Logger.getLogger(FileUtils.class);

	/**
	 * Reads a file into a {@link String}.
	 * 
	 * @param fileName the name or path of the file to read
	 * @return the {@link String} read from the file
	 */
	public static String readFileIntoString(String fileName) {
		logger.debug("Reading file into string: " + fileName);

		String result = "";
		try {
			result = readFileIntoString(new FileReader(fileName));
		} catch (FileNotFoundException e) {
			logger.error("Could not read file " + fileName, e);
		} catch (IOException e) {
			logger.error("Could not read file " + fileName, e);
		}

		return result;
	}

	/**
	 * Reads a file into a {@link String}.
	 * 
	 * @param file the file to read
	 * @return the {@link String} read from the file
	 */
	public static String readFileIntoString(File file) {
		logger.info("Reading file into string: " + file);

		String result = "";
		try {
			result = readFileIntoString(new FileReader(file));
		} catch (FileNotFoundException e) {
			logger.error("Could not read file " + file, e);
		} catch (IOException e) {
			logger.error("Could not read file " + file, e);
		}

		return result;
	}

	/**
	 * Reads a file into a {@link String}.
	 * 
	 * @param fileReader a <code>FileReader</code> of the file to read
	 * @return the {@link String} read from the file
	 */
	public static String readFileIntoString(FileReader fileReader) throws IOException {
		BufferedReader reader = null;
		reader = new BufferedReader(fileReader);

		String line;
		StringBuilder resultBuilder = new StringBuilder();
		while((line = reader.readLine()) != null) {
			resultBuilder.append(line);
			resultBuilder.append("\n");

			logger.debug("Read line: " + line);
		}

		try { reader.close(); } catch (Exception e) { /* ignore */ }
		return resultBuilder.toString();
	}

	/**
	 * Writes a {@link String} into a file.
	 * 
	 * @param fileName the file, including path, which is to write
	 * @param stringToWrite the {@link String} to write into the file
	 */
	public static void writeStringIntoFile(String fileName, String stringToWrite) {
		logger.info("Writing file: " + fileName);

		BufferedWriter writer = null;

		try {
			writer = new BufferedWriter(new FileWriter(fileName));

			writer.write(stringToWrite);
		} catch (IOException e) {
			logger.error("Could not write to file " + fileName, e);
		} finally {
			try { writer.close(); } catch (Exception e) { /* ignore */ }
		}
	}

	public static void createJar(String output, String packageName, String className) {
		logger.info("Compiling generated classes");
		JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
		
		String javaLocation = "src/main/java/" + packageName.replaceAll("\\.", "/") + "/" + className + ".java";
		if(compiler.run(null, null, null, javaLocation, "-d", "target/") != 0)
			logger.error("Could not compile generated classes");
		
		logger.info("Generating JAR file.");
		// Create Manifest
		Manifest manifest = new Manifest();
		manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
		manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, packageName + "." + className);

		JarOutputStream jarOutStream = null;
		try {
			jarOutStream = new JarOutputStream(new FileOutputStream(output), manifest);
			
			String classLocation = "target/" + packageName.replaceAll("\\.", "/") + "/" + className + ".class";
			addToJar(new File(classLocation), jarOutStream);
		} catch (FileNotFoundException e) {
			logger.error("Could not create JAR File", e);
		} catch (IOException e) {
			logger.error("Could not create JAR File", e);
		} finally {
			try { jarOutStream.close(); } catch (Exception e) { /*ignore*/ }
		}
	}

	private static void addToJar(File source, JarOutputStream jarOutStream) throws IOException {
		BufferedInputStream inputStream = null;
		try {
			// Remove "target/"
			JarEntry entry = new JarEntry(source.getPath().substring(7));
			entry.setTime(source.lastModified());
			jarOutStream.putNextEntry(entry);
			inputStream = new BufferedInputStream(new FileInputStream(source));

			byte[] buffer = new byte[1024];
			
			int count = 0;
			while ((count = inputStream.read(buffer)) != -1) {
				jarOutStream.write(buffer, 0, count);
			}
			
			jarOutStream.closeEntry();
		} finally {
			try { inputStream.close(); } catch (Exception e) { /*ignore*/ }
		}
	}
}