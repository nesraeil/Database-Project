package edu.yu.ds.SemesterProject;

import java.util.ArrayList;

import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.ColumnDescription;

public class ResultSet {
	//If select query, it holds all of the selected rows. 
	//Otherwise, holds a true or false value depending on whether a query was successful
	Table resultSet; 
	String queryType; //The quryType whos result this is
	ArrayList<ColumnDescription> columnDescriptions; 
	String error; //If the query failed, this is the error message
	
	
	/**
	 * Makes a resultSet for a create query and a select query
	 * @param table	The columns that were just created and their columns names
	 * @param queryType The quryType whos result this is
	 */
	ResultSet(Table resultTable, String queryType) {
		//if(queryType.startsWith("Create Table")) {
			
			columnDescriptions = new ArrayList<ColumnDescription>();
			for (int i = 0; i < resultTable.getColumnDescriptions().size(); i++) {
				columnDescriptions.add(resultTable.getColumnDescriptions().get(i));
			}
			
			resultSet = resultTable;
		//}
		
		error = "No Exceptions Were Thrown";
		this.queryType = queryType;
		//resultSet.printTable();//Take this out
	}
	
	/**
	 * Makes a resultSet for a successful query (which means no exceptions were thrown)
	 * @param success
	 * @param queryType
	 */
	ResultSet(Boolean success, String queryType) {
		Row result = new Row(success);
		resultSet = new Table(queryType);
		resultSet.insertRow(result);
		error = "No Exceptions Were Thrown";
		this.queryType = queryType;

	}
	
	/**
	 * Makes a resultSet for an unsuccessful query (which means an exception was thrown)
	 * @param success
	 * @param queryType
	 */
	ResultSet(Boolean success, String queryType, Exception error) {
		Row result = new Row(success);
		resultSet = new Table(queryType);
		resultSet.insertRow(result);
		this.queryType = queryType;
		if(queryType.equals("Parser Exception")) {
			this.error = "Invalid SQL Input";
		}
		else {
			this.error = error.getMessage();
		}

	}
	
	/**
	 * Prints out result set, using table.printTable()
	 */
	public void printResultSet() {
		System.out.println("**Result**");
		if(resultSet != null) {
			resultSet.printTable();
		}
		System.out.println(error);
		System.out.println("**End Result**\n");
	}
	
	public String getError() {
		return error;
	}
	
	public ArrayList<String> getColumnNames() {
		ArrayList<String> columnNames = new ArrayList<String>();
		for(ColumnDescription cd:columnDescriptions) {
			columnNames.add(cd.getColumnName());
		}
		return columnNames;
	}
	
	public ArrayList<String> getColumnTypes() {
		ArrayList<String> columnTypes = new ArrayList<String>();
		for(ColumnDescription cd:columnDescriptions) {
			columnTypes.add(cd.getColumnType().toString());
		}
		return columnTypes;
	}
	
	public Table getResult() {
		return resultSet;
	}
}
