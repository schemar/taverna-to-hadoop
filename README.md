# Taverna to Hadoop Compiler

Compiles Taverna workflows to native Hadoop java source code.

## Introduction

Taverna is an open source and domain-independent Workflow Management System – a suite of tools used to design and execute scientific workflows and aid in silico experimentation.
In order to enable scientists to easily scale their workflows created with Taverna, this compiler automatically generates a java class file that can be executed on Hadoop.

For more information see http://hadoop.apache.org/ or http://www.taverna.org.uk/

## Usage

Use -h or --help as program argument to get the help output.

## Demo

* Install Oracle Java (JDK 1.7)
* Install Taverna
* Install Eclipse
* Download/install Hadoop 1.0.3 from http://archive.apache.org/dist/hadoop/common/hadoop-1.0.3/

* In Eclipse
    * Check out from SCM: https://github.com/schenck/taverna-to-hadoop.git
    * Create package "generated" under "taverna_to_hadoop" (so you have "de.tuberlin.schenck.taverna_to_hadoop.generated")
    * Use Oracle Java, nothing else (OpenJDK will not work due to "ToolProvider.getSystemJavaCompiler()" returning "null")

* To test/demo the compiler:
    * In Eclipse, open the Run Configurations
    * Create a new one and name it whatever you want
         * As Project choose "taverna-to-hadoop"
         * As main class choose "de.tuberlin.schenck.taverna_to_hadoop.TavernaToHadoopMain.java"
         * If you want a simple demo, add the program arguments "-i resources/workflows/multiple_ports.t2flow -o MultipleWorkFlows.jar" (Of course you can choose another workflow or output name, -h prints the help)
    * Click on "Run"
    * Now the compiler will compile the workflow into a series of Hadoop jobs
         * It will convert the workflow to a linear list of map and reduce jobs
         * will create a class in the package "de.tuberlin.schenck.taverna_to_hadoop.generated" (See above. There will be a NullpointerException if you did not create the package)
         * It will package that class into a runnable .jar file
         * Please note that this is no "Uberjar", meaning the dependencies are not packaged into the jar. If you want to create an Uberjar to use with Hadoop, do the following:
             * In Eclipse, right click on the newly generated class file (in this case "de.tuberlin.schenck.taverna_to_hadoop.generated.MultipleWorkFlows.java")
             * "Run as ... -> Java Application"
             * The run will fail, because there is no Hadoop involved yet
             * Select "File->Export->Java->Runnabel JAR file", use the newly created "Launch configuration" (in this case "MultipleWorkFlows")
             * Choose to extract required libraries
             * Choose an export destination and export into the jar
             * In this example, the export will be to "taverna-to-hadoop/target/MultipleWorkFlows.jar"
    * Now we have an uberjar to run on Hadoop. For demo/testing purposes, there is already a folder "taverna-to-hadoop/testrun" that contains input for the workflow "multiple_ports.t2flow" used in this example
    * To run the generated jar as a Hadoop job, execute the following from within the "testrun" folder:
         * <path-to-hadoop-1.0.3>/bin/hadoop -jar <path-to-taverna-to-hadoop>/target/MultipleWorkFlows.jar
         * Now Hadoop will execute the series of jobs using the provided input in the folder "out" within "testrun"

* Inputs are all files within folders named "servicenameportname", e.g. onein1 for service "one" and port "in1"
* All inputs need to be in the folder "out"
* All inputs need to have key and value, key being the line number
* In this example, all inputs need to have the same number of lines

## Extending the Compiler

In the future it shall be possible to extend the compiler easily in order to incorporate new Taverna activities.
The compiler uses a template per activity approach to translate individual activities.

### Templates

Templates are java source files with some additional placeholders.
The engine will translate these files into pure java source files.
The file ending should be "jtemp", but does not matter.

The following placeholders are allowed within templates:

* <%= nameOfVariable %> (put the content of that variable at that position)
 * Available variables are:
 * hadoopClassName
 * hadoopPackageName
 * counter (a counter that increments every time it is called)
 * multipleOutputsRun
 * multipleOutputsWrite
 * A number of activity specific variables (see classes source codes in package de.tuberlin.schenck.taverna_to_hadoop.convert.activity_configs
* <%@ include file = "filename" %> (includes the content of file "filename" at that position)
* <%@ include mapreduce %> (puts the mapper and reducer classes at that position)
* <%@ include run %> (puts the run executions at that position)
* <%@ requires imports = "de.example,com.example.ex" %> (required imports to run this template)
* <%@ imports %> (mark the place where to put additional import statements)
