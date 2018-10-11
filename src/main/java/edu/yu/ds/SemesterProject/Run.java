package edu.yu.ds.SemesterProject;

import java.util.HashMap;

/**
 * If I ever catch any bad input to the database I throw an exception and then catch it in here,
 *  and put it into a result set which returns false and contains the error message.
 */

import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.*;

public class Run {
	private SQLParser sqlParser;//The thing that parses the sql and returns query objects
	private HashMap<String,Table> database;//Holds all of the tables that I have created. Key = Table Name
	
	/**
	 * Initializes an empty sqlParser and an empty database
	 */
	public Run() {
		sqlParser = new SQLParser();
		database = new HashMap<String, Table>();
	}
	
	/**
	 * 
	 * @param SQL Input from the user
	 * @return Returns a ResultSet for the specified query
	 */
	public ResultSet execute(String SQL) {
		ResultSet result = null;
		SQLQuery query = null;
		Boolean attempt = true; //Passed into the ResultSet. Switches to false if query fails.
		
		try { //If invalid sql, return a resultset to let user know
			 query = (SQLQuery)sqlParser.parse(SQL);
		} catch (Exception error) {
			attempt = false;
			result = new ResultSet(attempt, "Parser Exception" , error);
		}	
		
		/**
		 * Takes care of CreateTable Queries
		 */
		if (query instanceof CreateTableQuery) {
			CreateTableQuery tableQuery = (CreateTableQuery) query;
			try {
				Table newTable = new Table(tableQuery);
				database.put(tableQuery.getTableName(), newTable);
				result = new ResultSet(newTable, "Create Table Result");
			} catch (IllegalArgumentException error) {
				attempt = false;
				result = new ResultSet(attempt, "Create Table Result", error);
			}
		}
		
		/**
		 * Takes care of Insert Queries
		 */
		if (query instanceof InsertQuery) {
			InsertQuery insertQuery = (InsertQuery) query;
			Table currentTable = database.get(insertQuery.getTableName());
			try { //Fails if the table specified in the query doesn't exist
				if (currentTable == null) {
					String error = " Table named '" + insertQuery.getTableName() + "' does not exist ";
					throw new IllegalArgumentException(error);
				}
				HelperInsert insert = new HelperInsert(currentTable); //Takes care of everything insert related
				insert.insertRow(insertQuery);
				result = new ResultSet(attempt, "Insert Row Result");
			} catch (IllegalArgumentException error) {
				attempt = false;	
				result = new ResultSet(attempt, "Insert Row Result", error);
			}
		}
		
		/**
		 * Takes care of Select Queries
		 */
		if (query instanceof SelectQuery) {
			SelectQuery selectQuery = (SelectQuery) query;
			try {
				String[] tableToSelectFrom = selectQuery.getFromTableNames();
				if(tableToSelectFrom.length != 1) { //Can only select from one table at a time
					String error = "Select query must contain exactly 1 table to chose from";
					throw new IllegalArgumentException(error);
				}
				Table currentTable = database.get(tableToSelectFrom[0]);
				if (currentTable == null) { //And table in query must exist
					String error = " Table named '" + tableToSelectFrom[0] + "' does not exist ";
					throw new IllegalArgumentException(error);
				}
				HelperSelect helperSelect = new HelperSelect(currentTable); //Takes care of the select
				Table selectResult = helperSelect.select(selectQuery);
				result = new ResultSet(selectResult, "Select Result");
			} catch (IllegalArgumentException error) {
				attempt = false;	
				result = new ResultSet(attempt, "Select Row Failed", error);
			}
		}
		
		/**
		 * Takes care of Update Queries
		 */
		if (query instanceof UpdateQuery) {
			UpdateQuery updateQuery = (UpdateQuery) query;
			Table currentTable = database.get(updateQuery.getTableName());
			try {
				if (currentTable == null) {
					String e = " Table named '" + updateQuery.getTableName() + "' does not exist ";
					throw new IllegalArgumentException(e);
				}
				HelperUpdate update = new HelperUpdate(currentTable); //Takes care of update logic
				update.updateRow(updateQuery);
				result = new ResultSet(attempt, "Update Row Result");
			} catch (IllegalArgumentException e) {
				attempt = false;	
				result = new ResultSet(attempt, "Update Row Result", e);
			}
			
		}
		
		/**
		 * Takes care of Delete Queries
		 */
		if (query instanceof DeleteQuery) {
			DeleteQuery deleteQuery = (DeleteQuery) query;
			Table currentTable = database.get(deleteQuery.getTableName());
			try {
				if (currentTable == null) {
					String e = " Table named '" + deleteQuery.getTableName() + "' does not exist ";
					throw new IllegalArgumentException(e);
				}
				HelperDelete deleteHandler = new HelperDelete(currentTable); //Takec care of delete logic
				deleteHandler.deleteRows(deleteQuery);
				result = new ResultSet(attempt, "Delete Row Result");
			} catch (IllegalArgumentException e) {
				attempt = false;	
				result = new ResultSet(attempt, "Delete Row Result", e);
			}
		}
		
		/**
		 * Takes care of Index Queries
		 */
		if (query instanceof CreateIndexQuery) {
			CreateIndexQuery indexQuery = (CreateIndexQuery) query;
			Table currentTable = database.get(indexQuery.getTableName());
			try {
				if (currentTable == null) {
					String e = " Table named '" + indexQuery.getTableName() + "' does not exist ";
					throw new IllegalArgumentException(e);
				}
				HelperIndex indexHandler = new HelperIndex(currentTable); //Takes care of index logic
				indexHandler.createIndex(indexQuery.getColumnName());
				result = new ResultSet(attempt, "Index Row Result");
			} catch (IllegalArgumentException e) {
				attempt = false;	
				result = new ResultSet(attempt, "Index Row Result", e);
			}
		}
		return result; //Returns resultset
	}
}
