# Taverna to Hadoop Compiler

Compiles Taverna workflows to native Hadoop java source code.

## Introduction

Taverna is an open source and domain-independent Workflow Management System – a suite of tools used to design and execute scientific workflows and aid in silico experimentation.
In order to enable scientists to easily scale their workflows created with Taverna, this compiler automatically generates a java class file that can be executed on Hadoop.

For more information see http://hadoop.apache.org/ or http://www.taverna.org.uk/

## Usage

Use -h or --help as program argument to get the help output.

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
 * A number of activity specific variables
* <%@ include file = "filename" %> (includes the content of file "filename" at that position)
* <%@ include mapreduce %> (puts the mapper and reducer classes at that position)
* <%@ include run %> (puts the run executions at that position)
* <%@ requires imports = "de.example,com.example.ex" %> (required imports to run this part of code)
* <%@ imports %> (mark the place where to put additional import statements)