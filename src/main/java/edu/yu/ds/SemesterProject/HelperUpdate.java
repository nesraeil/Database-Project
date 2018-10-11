package edu.yu.ds.SemesterProject;

import java.util.ArrayList;
import java.util.HashMap;

import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.ColumnDescription;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.ColumnDescription.DataType;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.ColumnValuePair;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.UpdateQuery;

public class HelperUpdate {
	private Table toUpdate; //The table to perform the update on
	private HashMap<String, Integer> columnIndices; //Get what index in the list a column is based on column name
	ArrayList<ColumnValuePairValidated> validatedAndConvertedInput; //Contains all of the input that will be used to input, after it is validated
	private HashMap<String, ColumnDescription> columnDescriptionsByName; //Get the column description based on the column name

	/**
	 * Sets all of the fields, gets them from tableToUpdate
	 * @param toUpdate The table to Update
	 */
	HelperUpdate(Table toUpdate) {
		this.toUpdate = toUpdate;
		this.columnIndices = toUpdate.getcolumnIndex();
		validatedAndConvertedInput = new ArrayList<ColumnValuePairValidated>();
		columnDescriptionsByName = toUpdate.getColumnDescriptionsByName();
	}


	/**
	 * The logic of update queries.
	 * @param query UpdateQuery
	 */
	void updateRow(UpdateQuery query) {
		HelperWhere whereHandler = new HelperWhere(columnIndices);
		ArrayList<Row> updateDummy = toUpdate.cloneTable(); //Does update on this, and if anything fails before we finish, the original table is unaffected
		checkAndConvertValues(query.getColumnValuePairs()); //Makes sure that all values that we are using to update are valid input
		//If there is a where condition, and we are only dealing with indexed columns, do this
		if(query.getWhereCondition() != null && whereHandler.onlyHasIndexedColumns(query.getWhereCondition(), toUpdate.getIndexedColumns())) {
			updateDummy = whereHandler.checkAlreadyIndexedRows(query.getWhereCondition(), toUpdate); //Gets all of the columns using index
			ArrayList<Row> copy = new ArrayList<Row>();
			for(Row r:updateDummy) {
				copy.add(r.cloneRow());
			}
			for(int i = 0; i < updateDummy.size(); i++) {
				executeUpdate(updateDummy, i);
			}
		}
		else {//If columns arent indexed, just iterate through the whole table
			for(int i = 0; i < toUpdate.getTable().size(); i++) {
				if(query.getWhereCondition() == null || whereHandler.checkRowByWhere(query.getWhereCondition(), toUpdate.getRow(i))) {
					executeUpdate(updateDummy, i);
				}
			}
		}
	}
	
	/**
	 * once we have the column to update, put it through here. Updates table and index if needed
	 * @param updateDummy
	 * @param i
	 */
	void executeUpdate(ArrayList<Row> updateDummy, int i) {
		for(int j = 0; j < validatedAndConvertedInput.size();j++) {
			
			makeSureNoExceptionsWillHappen(updateDummy, i);
			String columnName = validatedAndConvertedInput.get(j).getColumnID().getColumnName();
			
			int insertIndex = columnIndices.get(columnName);
			
			Object updatedCell = validatedAndConvertedInput.get(j).getValue();
			updateIndex(columnName, updateDummy, i, insertIndex, updatedCell);
			toUpdate.getTable().get(i).replaceCell(insertIndex, updatedCell);

		}
	}
	
	/**
	 * Updates the index if an indexed column was changed
	 * @param columnName Column that was changed
	 * @param updateDummy List of updated rows
	 * @param i				Index of row we are at in table
	 * @param insertIndex	Index in table of updated column
	 * @param updatedCell	New, updated cell to add to index
	 */
	void updateIndex(String columnName, ArrayList<Row> updateDummy, int i, int insertIndex, Object updatedCell) {
		if(toUpdate.getIndexedColumns().containsKey(columnName)) {
			//Should delete the row from index
			toUpdate.getIndexedColumns().get(columnName).deleteSpecificRow(toUpdate.getTable().get(i).getCelltoString(insertIndex), toUpdate.getTable().get(i));
			updateDummy.get(i).replaceCell(insertIndex, updatedCell);
			ArrayList<Row> toInsert = new ArrayList<Row>();
			toInsert.add(updateDummy.get(i));
			if(toUpdate.getIndexedColumns().get(columnName).get(updatedCell.toString()) == null) {
				toUpdate.getIndexedColumns().get(columnName).put(updatedCell.toString(), toInsert);
			}
		}
	}
	
	/**
	 * Does a test update on a row, if any exceptions happen update will quit and nothing will be changed
	 */
	void makeSureNoExceptionsWillHappen(ArrayList<Row> updateDummy, int currentIndex) {
		for(int j = 0; j < validatedAndConvertedInput.size();j++) {
			String columnName = validatedAndConvertedInput.get(j).getColumnID().getColumnName();
			int insertIndex = columnIndices.get(columnName);
			Object updatedCell = validatedAndConvertedInput.get(j).getValue();
			if(columnDescriptionsByName.get(columnName).isUnique()) {
				checkUniqueness(columnName, updatedCell, updateDummy);
			}
			updateDummy.get(currentIndex).replaceCell(insertIndex, updatedCell);
		}
	}
	
	/**
	 * Makes sure that if column needs to be unique, it stays unique. Otherwise exits update.
	 */
	void checkUniqueness(String columnName, Object updatedCell, ArrayList<Row> updateDummy) {
		for(int i = 0;i < updateDummy.size();i++) {
			if(updateDummy.get(i).getCell(columnIndices.get(columnName)).equals(updatedCell)) {
				String error = "Can not update: Would cause column " + columnName + " to have duplicates";
				throw new IllegalArgumentException(error);
			}
		}
	}
	
	/**
	 * Makes sure that all update values are valid input
	 */
	private void checkAndConvertValues(ColumnValuePair[] cvp) {
		checkIfAllInputColumnsExist(cvp);
		checkNotNull(cvp);
		for(ColumnValuePair c:cvp) {
			validateAndConvertDataTypes(c);
		}		
	}
	
	/**
	 * Makes sure that every column that was entered into the input is one of the columns of the table
	 */
	private void checkIfAllInputColumnsExist(ColumnValuePair[] toInsert) {
		for(ColumnValuePair c:toInsert) {
			String columnNameToInsert = c.getColumnID().getColumnName();
			Boolean containsColumn = columnDescriptionsByName.containsKey(columnNameToInsert);
			if(containsColumn == false) {
				String error = "Column *" + columnNameToInsert + "* does not exist in table " + toUpdate.getTableName();
				throw new IllegalArgumentException(error);
			}
		}
	}
	/**
	 * Makes sure that if column shouldnt be null, no null input will be put in
	 */
	private void checkNotNull(ColumnValuePair[] columnsToInsert) {
		for(ColumnValuePair c: columnsToInsert) {
			Boolean isNull = false;
			String error = "Column *" + c.getColumnID().getColumnName() + "* in table *" + toUpdate.getTableName() + "* requires input";
			//If column should not be null, run this method. Otherwise skip
			if(columnDescriptionsByName.get(c.getColumnID().getColumnName()).isNotNull() == true || 
					c.getColumnID().getColumnName().equals(toUpdate.getPrimaryKeyColumn())) {
				//Make sure that the entry isn't an empty string
				if (c.getValue().trim().isEmpty()) {
					isNull = true;	
					error = "Entry to column *" + c.getColumnID().getColumnName() + "* in table *" + toUpdate.getTableName() + "* can't be empty";
				}
				//Making sure that the input isn't a plain string null (without apostrophes around it)
				else if (c.getValue().trim().equals("null")) {
					isNull = true;
					error = "Entry to column *" + c.getColumnID().getColumnName() + "* in table *" + toUpdate.getTableName() + "* can't be null ";					}
				}
			if (isNull == true) {
				throw new IllegalArgumentException(error);
			}
		}	
	}
	
	/**
	 * Makes sure that all update input is valid for the column its supposed to go in
	 */
	void validateAndConvertDataTypes(ColumnValuePair cvp) {
		ColumnDescription columnToCheckAgainst = columnDescriptionsByName.get(cvp.getColumnID().getColumnName());
		DataType inputType = columnToCheckAgainst.getColumnType();
		//This only executes if there is input
		if(cvp.getValue() != null && !(cvp.getValue().trim().isEmpty())) {
			String input = cvp.getValue().trim();
			switch (inputType) {
				case VARCHAR:
					validateAndConvertVarchar(input, columnToCheckAgainst, cvp);
					break;
				case DECIMAL:
					try {
						validateAndConvertDecimal(input, columnToCheckAgainst, cvp);
					} catch (NumberFormatException e) {
						String error = "You entered *" + input + "*. Input to column *" + columnToCheckAgainst.getColumnName() + "* in table *" + toUpdate.getTableName() 
								+ "* must be a decimal number";
						throw new IllegalArgumentException(error);
					} catch (IllegalArgumentException e) {
						throw new IllegalArgumentException(e.getMessage());
					}
					break;
				case BOOLEAN:
					validateAndConvertBoolean(input, columnToCheckAgainst, cvp);
					break;
				case INT:
					validateAndConvertInteger(input, columnToCheckAgainst, cvp);
					break;
			}
		}
	}
	
	/**
	 * If varchar row, makes sure that everything is as it should be
	 */
	private void validateAndConvertVarchar(String input, ColumnDescription columnToCheckAgainst, ColumnValuePair cvp) {
		//All varchar input must be surrounded by ' '
		if (input.charAt(0) != '\'' && input.charAt(input.length() - 1) != '\'') {
			String e = "You entered " + input + ". Column *" + columnToCheckAgainst.getColumnName() + "* in table *" + toUpdate.getTableName() 
					+ "* requires input to be a string surrounded by 'apostrophes'";
			throw new IllegalArgumentException(e);
		} 
		//VarChar value can'e be longer than original stated length for column
		else if (input.length() - 2 > columnToCheckAgainst.getVarCharLength()) {
			String e = "You entered *" + input + "*. Input to column *" + columnToCheckAgainst.getColumnName() + "* in table *" + toUpdate.getTableName() 
					+ "* must be " + columnToCheckAgainst.getVarCharLength() + " characters or shorter";
			throw new IllegalArgumentException(e);
		}
		ColumnValuePairValidated finalVarcharCvp = new ColumnValuePairValidated(cvp.getColumnID(), input.substring(1,input.length() -1));
		validatedAndConvertedInput.add(finalVarcharCvp);
	}
	
	/**
	 * If decimal row, makes sure that everything is as it should be
	 */
	private void validateAndConvertDecimal(String input, ColumnDescription columnToCheckAgainst, ColumnValuePair cvp) {
		Double value = Double.parseDouble(input);
		int decimalIndex = -1;
		int wholeNumberLength = 0;
		int fractionLength = 0;
		//May need to throw exception if there is a number with a dot and no numbers after
		//I misunderstood this...check up on what the return values of whole and fraction are
		if(input.contains(".")) {
			decimalIndex = input.indexOf('.');
			wholeNumberLength = input.substring(0, decimalIndex).length();
			fractionLength =  input.substring(decimalIndex + 1).length();
		}
		else {
			wholeNumberLength = input.length();
		}
		if(wholeNumberLength > columnToCheckAgainst.getWholeNumberLength()) {
			String error = "You entered *" + input + "*. Input to column *" + columnToCheckAgainst.getColumnName() + "* in table *" + toUpdate.getTableName() 
					+ "* can not have be more than " + columnToCheckAgainst.getWholeNumberLength() + " digits before the decimal";
			throw new IllegalArgumentException(error);
		}
		else if(fractionLength > columnToCheckAgainst.getFractionLength()) {
			String error = "You entered *" + input + "*. Input to column *" + columnToCheckAgainst.getColumnName() + "* in table *" + toUpdate.getTableName() 
					+ "* can not have be more than " + columnToCheckAgainst.getFractionLength() + " digit(s) after the decimal";
			throw new IllegalArgumentException(error);
		}
		
		String strDouble = String.format("%."+columnToCheckAgainst.getFractionLength()+"f", value);//Check if this works			
		Double finalDoubleInput = Double.parseDouble(strDouble);//Now what...?
		ColumnValuePairValidated finalDoubleCvp = new ColumnValuePairValidated(cvp.getColumnID(), finalDoubleInput);
		validatedAndConvertedInput.add(finalDoubleCvp);	
	}
	
	/**
	 * If boolean row, makes sure that everything is as it should be
	 */
	private void validateAndConvertBoolean(String input, ColumnDescription columnToCheckAgainst, ColumnValuePair cvp) { 
		if (!input.equalsIgnoreCase("true") && !input.equalsIgnoreCase("false")) {
			String error = "You entered *" + input + "*. Input to column *" + columnToCheckAgainst.getColumnName() + "* in table *" + toUpdate.getTableName() 
					+ "* must be \"true\" or \"false\"";	
			throw new IllegalArgumentException(error);
		}
		Boolean finalBooleanInput = Boolean.parseBoolean(input);
		ColumnValuePairValidated finalBooleanCvp = new ColumnValuePairValidated(cvp.getColumnID(), finalBooleanInput);
		validatedAndConvertedInput.add(finalBooleanCvp);
	}
	
	/**
	 * If Integer row, makes sure that everything is as it should be
	 */
	private void validateAndConvertInteger(String input, ColumnDescription columnToCheckAgainst, ColumnValuePair cvp) { 
		try {
			Integer.parseInt(input);
		} catch (NumberFormatException e) {
			String error = "You entered *" + input + "*. Input to column *" + columnToCheckAgainst.getColumnName() + "* in table *" + toUpdate.getTableName() 
					+ "* must be an integer";
			throw new IllegalArgumentException(error);
		}
		Integer finalIntInput = Integer.parseInt(input);
		ColumnValuePairValidated finalIntCvp = new ColumnValuePairValidated(cvp.getColumnID(), finalIntInput);
		validatedAndConvertedInput.add(finalIntCvp);
	}
}
