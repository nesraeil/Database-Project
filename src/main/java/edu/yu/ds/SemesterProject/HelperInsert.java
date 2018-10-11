package edu.yu.ds.SemesterProject;

import java.util.ArrayList;
import java.util.HashMap;

import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.ColumnDescription;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.ColumnValuePair;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.InsertQuery;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.ColumnDescription.DataType;

class HelperInsert {
	//All of the same fields from my other classes
	private String tableName;
	private String primaryKeyColumn;
	private HashMap<String, ColumnDescription> columnDescriptionsByName;
	private ArrayList<ColumnDescription> columnDescriptions;
	private ArrayList<Row> table;
	private ArrayList<ColumnValuePairValidated> validatedAndConvertedInput;
	private Table toInsertTo;
	
	/**
	 * 
	 * @param toInsertTo The table passed in by the query
	 */
	HelperInsert(Table toInsertTo) {
		this.toInsertTo = toInsertTo;
		table = toInsertTo.getTable();
		columnDescriptions = toInsertTo.getColumnDescriptions();
		validatedAndConvertedInput = new ArrayList<ColumnValuePairValidated>();
		primaryKeyColumn = toInsertTo.getPrimaryKeyColumn();
		columnDescriptionsByName = toInsertTo.getColumnDescriptionsByName();
		tableName = toInsertTo.getTableName();
	}
	
	
	/**
	 * Takes insert query, checks the input, and if everything checks 
	 * out then the input is inserted into the table
	 * 
	 * @param toInsert	The insert query returned from the parser
	 */
	void insertRow(InsertQuery toInsert) {
		validatedAndConvertedInput = new ArrayList<ColumnValuePairValidated>();
		checkInsertInput(toInsert.getColumnValuePairs());
		Row rowToInsert = new Row();
		for(int i = 0; i < columnDescriptions.size(); i++) {
			for(int j = 0; j < validatedAndConvertedInput.size(); j++) {
				if (columnDescriptions.get(i).getColumnName().equals(validatedAndConvertedInput.get(j).getColumnID().getColumnName())) {
					rowToInsert.addCell(validatedAndConvertedInput.get(j).getValue());	
					j = validatedAndConvertedInput.size();
				}
				else if (j == validatedAndConvertedInput.size() - 1 
						&& columnDescriptions.get(i).getHasDefault()) {
					//Set default value
					rowToInsert.addCell(setColumnToDefaultValue(columnDescriptions.get(i)));
				}
				else if (j == validatedAndConvertedInput.size() - 1)  {
					rowToInsert.addCell(null);
				}
			}	
		}
		table.add(rowToInsert);
		insertToIndex(rowToInsert);
	}
	
	/**
	 * Inserts the new row into the index for the columns which are indexed
	 * @param rowToInsert
	 */
	void insertToIndex(Row rowToInsert) {
		HashMap<String, Integer> columnIndices = toInsertTo.getcolumnIndex();
		for(String index:toInsertTo.getIndexedColumns().keySet()) {
			int columnIndex = columnIndices.get(index);
			ArrayList<Row> toInsert = new ArrayList<Row>();
			toInsert.add(rowToInsert);
			ArrayList<Row> listAtIndexValue = toInsertTo.getIndexedColumns().get(index).get(rowToInsert.getCelltoString(columnIndex));
			if(listAtIndexValue != null) {
				listAtIndexValue.addAll(toInsert);
			}
			else {
				toInsertTo.getIndexedColumns().get(index).put(rowToInsert.getCelltoString(columnIndex), toInsert);
			}
		}
	}
	
	/**
	 * If the entry into a column is null, this sets the value to the columns default value
	 * @param column
	 * @return
	 */
	Object setColumnToDefaultValue(ColumnDescription column) {
		//Not sure, but I may have to do more validation to make sure that default values have correct input
		String defaultValue = column.getDefaultValue();
		DataType defaultValueType = column.getColumnType();
		
		switch (defaultValueType) {
		case VARCHAR:
			return defaultValue.substring(1,defaultValue.length() - 1);//Assumes that default value is surrounded by ' '	
		case DECIMAL:
			return Double.parseDouble(defaultValue);
		case BOOLEAN:
			return Boolean.parseBoolean(defaultValue);
		case INT:
			return Integer.parseInt(defaultValue);
		}
		return null;//Should ever get to this	
	}

	/**
	 * The driver for all of the things we have to check
	 * @param toCheck
	 */
	private void checkInsertInput(ColumnValuePair[] cvpToCheck) {
		HashMap<String, ColumnValuePair> cvpByColumnName = new HashMap<String, ColumnValuePair>();
		validatedAndConvertedInput = new ArrayList<ColumnValuePairValidated>();
		for(ColumnValuePair c: cvpToCheck) {//Makes a hashmap of all the column value pairs by name
			cvpByColumnName.put(c.getColumnID().getColumnName(), c);
		}
		checkPrimaryKeyNotNull(cvpByColumnName);//Self explanatory
		checkIfAllInputColumnsExist(cvpToCheck);//Throws exception of an inputted column doesnt exist
		for(ColumnDescription cd: columnDescriptions) {//Checks that any nonnull/unique columns have the correct input
			checkNotNull(cd,cvpByColumnName);
			checkUnique(cd,cvpByColumnName);
			
		}
		for (int i = 0; i < cvpToCheck.length; i++) {
			validateAndConvertDataTypes(cvpToCheck[i]);//Makes sure all of the data is all good
		}
	}
	/**
	 * Checks to make sure that primary key input exists, and therefore is not null
	 */
	private void checkPrimaryKeyNotNull(HashMap<String, ColumnValuePair> cvp) {
		Boolean hasPrimaryKey = false;
		ColumnValuePair value = cvp.get(primaryKeyColumn);
		if(value != null) {
			hasPrimaryKey = true;
		}
		
		if (hasPrimaryKey == false) {
			String error = "Every Line Must Contain a Primary Key";
			throw new IllegalArgumentException(error);
		}
	}
	/**
	 * Makes sure that every column that was entered into the input is one of the columns of the table
	 */
	void checkIfAllInputColumnsExist(ColumnValuePair[] toInsert) {
		//See if i can get rid of loop by utilizing column description hashmap
		for(ColumnValuePair c:toInsert) {
			String columnNameToInsert = c.getColumnID().getColumnName();
			Boolean containsColumn = columnDescriptionsByName.containsKey(columnNameToInsert);
			if(containsColumn == false) {
				String error = "Column *" + columnNameToInsert + "* does not exist in table " + tableName;
				throw new IllegalArgumentException(error);
			}
		}
	}
	/**
	 * Makes sure that all columns that shouldn't be null have input
	 */
	private void checkNotNull(ColumnDescription columnToCheckAgainst, HashMap<String, ColumnValuePair> cvp) {
		//If column should not be null, run this method. Otherwise skip.
		if (columnToCheckAgainst.isNotNull() == true || columnToCheckAgainst.getColumnName().equals(primaryKeyColumn)) {
			Boolean isNull = false;
			Boolean columnWasNotInInput = true;
			String error = "Column *" + columnToCheckAgainst.getColumnName() + "* in table *" + tableName + "* requires input";
			
			ColumnValuePair value = cvp.get(columnToCheckAgainst.getColumnName());
			if (value != null) {
				columnWasNotInInput = false;
				//Make sure that the entry isn't an empty string
				if (value.getValue().trim().isEmpty()) {
					isNull = true;	
					error = "Entry to column *" + columnToCheckAgainst.getColumnName() + "* in table *" + tableName + "* can't be empty";
				}
				//Making sure that the input isn't a plain string null (without apostrophes around it)
				else if (value.getValue().trim().equals("null")) {
					isNull = true;
					error = "Entry to column *" + columnToCheckAgainst.getColumnName() + "* in table *" + tableName + "* can't be null ";					}
				}
			if (isNull == true || columnWasNotInInput == true) {
				throw new IllegalArgumentException(error);
			}
		}		
	}
	
	/**
	 * Checking if any of the input needs to be unique. If so, it checks if the row it is
	 * being inserted too contains the input yet
	 */
	private void checkUnique(ColumnDescription columnToCheckAgainst, HashMap<String, ColumnValuePair> cvp) {
		Boolean columnIsInInput = false;
		ColumnValuePair inputColumnToCheck = null;
		ColumnValuePair value = cvp.get(columnToCheckAgainst.getColumnName());
		if (value != null) {
			columnIsInInput = true;
			inputColumnToCheck = value;
		}
		if(columnIsInInput == true && (columnToCheckAgainst.isUnique() == true || value.getColumnID().getColumnName().equals(primaryKeyColumn))) {
			int indexToCheck = 0;
			//This loop to find what index a column is...
			//Check back on if I could do this better
			for(int i = 0; i < columnDescriptions.size(); i ++) {
				if(columnDescriptions.get(i).getColumnName().equals(columnToCheckAgainst.getColumnName())){
					indexToCheck = i;
					i = columnDescriptions.size();
				}
			}
			//This loop goes through table at index to see if value is already in the table..
			for(Row r:table) {
				if (r.getCelltoString(indexToCheck).equals(inputColumnToCheck.getValue())) {
					String e = "You entered *" + inputColumnToCheck.getValue() + "*. Column *" + columnToCheckAgainst.getColumnName() 
						+ "* in table *" + tableName + "* can't have duplicate input. ";
					throw new IllegalArgumentException(e);
				}
			}
		}
	}
	
	/**
	 * The step of the checking process to check that every input
	 * conforms to the correct rules of the row that it's being put into
	 * 
	 * @param cvp The specific columnValuePair to check 
	 */
	void validateAndConvertDataTypes(ColumnValuePair cvp) {
		ColumnDescription columnToCheckAgainst = columnDescriptionsByName.get(cvp.getColumnID().getColumnName());
		DataType inputType = columnToCheckAgainst.getColumnType();
		//This only happens if there is input
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
						String error = "You entered *" + input + "*. Input to column *" + columnToCheckAgainst.getColumnName() + "* in table *" + tableName 
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
	 * Makes sure that varchar input is what it should be and converts it
	 */
	private void validateAndConvertVarchar(String input, ColumnDescription columnToCheckAgainst, ColumnValuePair cvp) {
		//All varchar input must be surrounded by ' '
		if (input.charAt(0) != '\'' && input.charAt(input.length() - 1) != '\'') {
			String e = "You entered " + input + ". Column *" + columnToCheckAgainst.getColumnName() + "* in table *" + tableName 
					+ "* requires input to be a string surrounded by 'apostrophes'";
			throw new IllegalArgumentException(e);
		} 
		//VarChar value can'e be longer than original stated length for column
		else if (input.length() - 2 > columnToCheckAgainst.getVarCharLength()) {
			String e = "You entered *" + input + "*. Input to column *" + columnToCheckAgainst.getColumnName() + "* in table *" + tableName 
					+ "* must be " + columnToCheckAgainst.getVarCharLength() + " characters or shorter";
			throw new IllegalArgumentException(e);
		}
		ColumnValuePairValidated finalVarcharCvp = new ColumnValuePairValidated(cvp.getColumnID(), input.substring(1,input.length() -1));
		validatedAndConvertedInput.add(finalVarcharCvp);
	}
	/**
	 * Makes sure that decimal input is what it should be and converts it
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
			String error = "You entered *" + input + "*. Input to column *" + columnToCheckAgainst.getColumnName() + "* in table *" + tableName 
					+ "* can not have be more than " + columnToCheckAgainst.getWholeNumberLength() + " digits before the decimal";
			throw new IllegalArgumentException(error);
		}
		else if(fractionLength > columnToCheckAgainst.getFractionLength()) {
			String error = "You entered *" + input + "*. Input to column *" + columnToCheckAgainst.getColumnName() + "* in table *" + tableName 
					+ "* can not have be more than " + columnToCheckAgainst.getFractionLength() + " digit(s) after the decimal";
			throw new IllegalArgumentException(error);
		}
		String strDouble = String.format("%."+columnToCheckAgainst.getFractionLength()+"f", value);//Check if this works				
		Double finalDoubleInput = Double.parseDouble(strDouble);//Now what...?
		//Make an array of cvp and return it
		ColumnValuePairValidated finalDoubleCvp = new ColumnValuePairValidated(cvp.getColumnID(), finalDoubleInput);
		validatedAndConvertedInput.add(finalDoubleCvp);	
	}
	/**
	 * Makes sure that boolean input is what it should be and converts is
	 */
	private void validateAndConvertBoolean(String input, ColumnDescription columnToCheckAgainst, ColumnValuePair cvp) { 
		if (!input.equalsIgnoreCase("true") && !input.equalsIgnoreCase("false")) {
			String error = "You entered *" + input + "*. Input to column *" + columnToCheckAgainst.getColumnName() + "* in table *" + tableName 
					+ "* must be \"true\" or \"false\"";	
			throw new IllegalArgumentException(error);
		}
		Boolean finalBooleanInput = Boolean.parseBoolean(input);
		ColumnValuePairValidated finalBooleanCvp = new ColumnValuePairValidated(cvp.getColumnID(), finalBooleanInput);
		validatedAndConvertedInput.add(finalBooleanCvp);
	}
	/**
	 * Makes sure that Integer input is what it should be and converts is
	 */
	private void validateAndConvertInteger(String input, ColumnDescription columnToCheckAgainst, ColumnValuePair cvp) { 
		try {
			Integer.parseInt(input);
		} catch (NumberFormatException e) {
			String error = "You entered *" + input + "*. Input to column *" + columnToCheckAgainst.getColumnName() + "* in table *" + tableName 
					+ "* must be an integer";
			throw new IllegalArgumentException(error);
		}
		Integer finalIntInput = Integer.parseInt(input);
		ColumnValuePairValidated finalIntCvp = new ColumnValuePairValidated(cvp.getColumnID(), finalIntInput);
		validatedAndConvertedInput.add(finalIntCvp);
	}
}
