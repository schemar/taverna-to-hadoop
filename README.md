# Taverna to Hadoop Compiler

## Introduction

## Usage

## Templates

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