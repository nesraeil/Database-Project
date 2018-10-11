package edu.yu.ds.SemesterProject;


import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;

import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.ColumnDescription;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.ColumnDescription.DataType;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.CreateTableQuery;


class Table {
	
	private String tableName; //Name of table
	private String primaryKeyColumn; //Name of primary key column
	private HashMap<String, ColumnDescription> columnDescriptionsByName; //To Get column descriptions if you have the name
	private ArrayList<ColumnDescription> columnDescriptions; //A list of column descriptions in the order they were passed in
	private ArrayList<Row> table;//Contains all of the table rows
	private HashMap<String,BTree<String,ArrayList<Row>>> indexedColumns; //Contains all of the btree indexed columns
	private HashMap<String, Integer> columnIndices; //Contains the list indexed of every column

	
	/**
	 * Creates a table from a createTableQuery
	 * Checks that all input is how it should be
	 * 
	 * @param query	The create table query that is passed in
	 */
	Table(CreateTableQuery query) {
		tableName = query.getTableName();
		primaryKeyColumn = query.getPrimaryKeyColumn().getColumnName();
		columnDescriptionsByName = new HashMap<String, ColumnDescription>();
		columnDescriptions = new ArrayList<ColumnDescription>();
		indexedColumns = new HashMap<String, BTree<String,ArrayList<Row>>>();
		columnIndices = new HashMap<String, Integer>();
		table = new ArrayList<Row>();
		int i = 0;
		for(ColumnDescription c:query.getColumnDescriptions()) {
			if(c.getColumnName().equals(primaryKeyColumn)) { //Setting primary key properties
				c.setNotNull(true);
				c.setUnique(true);
			}
			columnDescriptionsByName.put(c.getColumnName(), c);//Adding the column descriptions to the various containers
			columnDescriptions.add(c);
			columnIndices.put(c.getColumnName(), i);
			checkDefaultValues(c);
			i++;
		}
		checkPrimaryKeyNoDefault(); //THrows an error if the primary column has a default value
		HelperIndex indexHandler = new HelperIndex(this);
		indexHandler.createIndex(primaryKeyColumn); //Makes a btree index for the primary key column
	}
	/**
	 * Makes sure all default values are good
	 */
	private void checkDefaultValues(ColumnDescription cd) {
		String defaultValue = cd.getDefaultValue();
		if(defaultValue != null && cd.getColumnType() != null) {
			if(cd.getColumnType().equals(DataType.BOOLEAN)) {
				if(!defaultValue.equals("true") && !defaultValue.equals("false")) {
					throw new IllegalArgumentException("Bad input for defalut value of " + cd.getColumnName());
				}
				
			}
			if(cd.getColumnType().equals(DataType.DECIMAL)) {
				try {
					Double.parseDouble(defaultValue);
				} catch (Exception e) {
					throw new IllegalArgumentException("Bad input for defalut value of " + cd.getColumnName());
				}	
			}
			if(cd.getColumnType().equals(DataType.INT)) {
				try {
					Integer.parseInt(defaultValue);
				} catch (Exception e) {
					throw new IllegalArgumentException("Bad input for defalut value of " + cd.getColumnName());			
				}
			}
		}
		
	}
	/**
	 * Makes sure the primary key column doesnt have a default value
	 */
	void checkPrimaryKeyNoDefault() {
		if(columnDescriptionsByName.get(primaryKeyColumn).getHasDefault()) {
			String e = "Primary Key column can't have a default value";
			throw new IllegalArgumentException(e);
		}				
	}
	
	HashMap<String,BTree<String,ArrayList<Row>>> getIndexedColumns() {
		return indexedColumns;
	}
	
	void addIndexedRow(BTree<String,ArrayList<Row>> index, String indexName) {
		indexedColumns.put(indexName,index);
	}
	/**
	 * Returns true if the specified column has an index already
	 */
	boolean isColumnIndexed(String columnName) {
		if(indexedColumns.get(columnName) != null) {
			return true;
		}
		return false;
	}
	/**
	 * Creates a table for use in resultSet, and therefore some fields are not set
	 * 
	 * @param row 		Should be a row containing the single true/false result value
	 * @param tableName	The name of the query type that this result is coming from
	 */
	Table(String tableName) {
		this.tableName = tableName;
		table = new ArrayList<Row>();	
	}
	
	/**
	 * Creates table for use as a select result
	 * @param selections	The selected information
	 * @param tableName		Will always be "Select Result from -Table Name-"
	 * @param selectedColumnDescriptions	Contains only the column descriptions of the selected rows
	 */
	Table(ArrayList<Row> selections, String tableName, ArrayList<ColumnDescription> selectedColumnDescriptions) {
		table = selections;
		this.tableName = tableName;
		columnDescriptions = selectedColumnDescriptions;
	}
	
	/**
	 * Creates a table with input from another table
	 */
	
	Table(Table newTable) {
		this.tableName = newTable.getTableName();
		primaryKeyColumn = newTable.getPrimaryKeyColumn();
		columnDescriptionsByName = newTable.getColumnDescriptionsByName();
		columnDescriptions = newTable.getColumnDescriptions();
		table = newTable.getTable();
		columnIndices = newTable.getcolumnIndex();
	}

	/**
	 * @return The list index of a column
	 */
	HashMap<String, Integer> getcolumnIndex() {
		return columnIndices;
	}
	
	String getPrimaryKeyColumn() {
		return primaryKeyColumn;
	}
	
	HashMap<String, ColumnDescription> getColumnDescriptionsByName() {
		return columnDescriptionsByName;
	}
	String getTableName() {
		return tableName;
	}
	
	/**
	 * Use this to add a row if you already have a row to add
	 * ie. (For resultSet and in the update method)
	 */
	void insertRow(Row row) {
		table.add(row);
	}
	/**
	 * Get a specific row if you already know its index
	 */
	Row getRow(int index) {
		return table.get(index);
	}
	
	ArrayList<Row> getTable() {
		return this.table;
	}
	/**
	 * Deletes all rows from table
	 */
	void deleteAllRows() {
		table = new ArrayList<Row>();
	}
	
	/**
	 *Gets the list of column Descriptions in this table
	 */
	ArrayList<ColumnDescription> getColumnDescriptions() {
		return columnDescriptions;
	}
	
	/**
	 * Makes a copy of a table, with new row objects that have the same values
	 * @return The copy of the table
	 */
	ArrayList<Row> cloneTable() {
		ArrayList<Row> copy = new ArrayList<Row>();
		for(Row r:this.table) {
			copy.add(r.cloneRow());
		}
		return copy;
	}
	
	/**
	 * DReturns a table without the deleted row
	 * @param r
	 */
	void deleteRow(Row r) {
		ArrayList<Row> newTable = new ArrayList<Row>();
		for(Row toAdd:table) {
			if (toAdd != r) newTable.add(toAdd);
		}
		table.clear();
		table.addAll(newTable);
		
	}
	
	/**
	 * For the use of resultset, and for debugging
	 */
	void printTable() {
		StringBuilder columns = new StringBuilder();
		if (table != null) {
			System.out.println("Name:" + tableName);
		}
		if (columnDescriptions != null) {
			for(int i = 0;i < columnDescriptions.size();i++) {
				columns.append("|" + StringUtils.center(columnDescriptions.get(i).getColumnName(), 16));
			}
			columns.append("|");
			String rowLines = "";
			for(int i = 0; i < columns.toString().length(); i++) {
				rowLines += "-";
			}
			System.out.println(rowLines);
			System.out.println(columns.toString());
			System.out.println(rowLines);
		}
		if (table != null) {
			for(int i = 0;i < table.size(); i++) {
				for(int j = 0; j < table.get(i).size();j++) {
					System.out.printf("|%s", StringUtils.center(table.get(i).getCelltoString(j), 16));
				}
				System.out.print("|\n");
			}
		}
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((table == null) ? 0 : table.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Table other = (Table) obj;
		if (table == null) {
			if (other.table != null)
				return false;
		} else if (!table.equals(other.table))
			return false;
		return true;
	}	
	
	
}
