package edu.yu.ds.SemesterProject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.ColumnDescription;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.ColumnDescription.DataType;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.ColumnID;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.Condition;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.SelectQuery;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.SelectQuery.FunctionInstance;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.SelectQuery.OrderBy;

public class HelperSelect {
	private Table selectFrom;
	private ArrayList<ColumnDescription> selectedColumnDescriptions;
	private HashMap<String, Integer> columnIndices;
	private HashMap<String, ColumnDescription> allColumnDescriptionsByName;
	
	/**
	 * Sets all fields based in table to select from
	 */
	HelperSelect(Table selectFrom) {
		this.selectFrom = selectFrom;
		this.columnIndices = selectFrom.getcolumnIndex();
		allColumnDescriptionsByName = selectFrom.getColumnDescriptionsByName();
		selectedColumnDescriptions = new ArrayList<ColumnDescription>();
	}
	
	/**
	 * Runs the whole select logic
	 */
	Table select(SelectQuery query) {
		ArrayList<Row> selectBuilder = new ArrayList<Row>();//Will ultimately be the table to return
		setSelectedColumnDescriptions(query.getSelectedColumnNames()); //The columns that are to be selected
		selectBuilder = getRowsByWhere(query.getWhereCondition());//Gets rows which are chosen by the where condition
		if(selectBuilder == null) {
			return new Table(selectBuilder, "Selected From " + query.getFromTableNames()[0], selectedColumnDescriptions);
		}
		orderBy(selectBuilder, query.getOrderBys());
		selectBuilder = trimExcessColumns(selectBuilder); //Cuts off non selected columns and orders columns correctly
		function(selectBuilder, query.getFunctions(), query.getSelectedColumnNames());//Do functions
		selectBuilder = distinct(selectBuilder, query.isDistinct());//Do distinct
		return new Table(selectBuilder, "Selected From " + query.getFromTableNames()[0], selectedColumnDescriptions);
	}
	
	/**
	 * If it needs to be distict, do not return and duplicate rows
	 */
	private ArrayList<Row> distinct(ArrayList<Row> tableToDistinct, boolean isDistinct) {
		if(!isDistinct) return tableToDistinct;
		HashSet<Row> distinctCheck = new HashSet<Row>();
		ArrayList<Row> distinctResult = new ArrayList<Row>();
		for(Row currentRow:tableToDistinct) {
			boolean distinct = distinctCheck.add(currentRow);
			if(distinct) {
				distinctResult.add(currentRow);
			}
		}
		return distinctResult;
	}
	
	/**
	 * RUns through columns, if one needs to be functioned, pass it to executeFunction()
	 */
	private void function(ArrayList<Row> toFunctionize, ArrayList<FunctionInstance> functionedColumns, ColumnID[] selectedColumnIds) {
		if(functionedColumns == null) return;
		for(FunctionInstance fn:functionedColumns) {
			for(int i = 0;i < selectedColumnIds.length;i++) {
				if(fn.column == selectedColumnIds[i]) {
					executeFunction(toFunctionize, fn, i);
				}
			}
		}
		
	}
	
	/**
	 * Sorts out what function we are dealing with, and sends it to the right function method
	 */
	private void executeFunction(ArrayList<Row> toFunctionize, FunctionInstance functionInstance, int columnIndex) {
		ColumnDescription selectedCD = selectedColumnDescriptions.get(columnIndex);
		switch (functionInstance.function) {
			case AVG:
				if(selectedCD.getColumnType() == DataType.VARCHAR || selectedCD.getColumnType() == DataType.BOOLEAN) {
					throw new IllegalArgumentException("Can't get average of " + selectedCD.getColumnName());
				}
				else doAverage(toFunctionize, columnIndex, functionInstance.isDistinct);			
				break;
			case SUM:
				if(selectedCD.getColumnType() == DataType.VARCHAR || selectedCD.getColumnType() == DataType.BOOLEAN) {
					throw new IllegalArgumentException("Can't get sum of " + selectedCD.getColumnName());
				}
				else doSum(toFunctionize, columnIndex, functionInstance.isDistinct);	
				break;
			case MAX:
				if(selectedCD.getColumnType() == DataType.BOOLEAN) {
					throw new IllegalArgumentException("Can't get maximum of " + selectedCD.getColumnName());
				}
				else doMax(toFunctionize, columnIndex, functionInstance.isDistinct);
				break;
			case MIN:
				if(selectedCD.getColumnType() == DataType.BOOLEAN) {
					throw new IllegalArgumentException("Can't get minimum of " + selectedCD.getColumnName());
				}
				else doMin(toFunctionize, columnIndex, functionInstance.isDistinct);
				break;
			case COUNT:
				doCount(toFunctionize, columnIndex, functionInstance.isDistinct);			
				break;
		}
	}
	/**
	 * Counts rows in specified column, does only distinct if necessary
	 */
	private void doCount(ArrayList<Row> toFunctionize, int columnIndex, boolean isDistinct) {
		int count = 0;
		HashSet<Object> distinctCheck = new HashSet<Object>();
		//I am not including null values in the count
		if(isDistinct == true) {
			for(Row r:toFunctionize) {
				if(r.getCell(columnIndex) != null) distinctCheck.add(r.getCell(columnIndex));
			}
			count = distinctCheck.size();
		}
		else {
			for(Row r:toFunctionize) {
				if(r.getCell(columnIndex) != null) count++;
			}
		}
		for(Row r: toFunctionize) {
			r.replaceCell(columnIndex, count);
		}
	}
	/**
	 * Averages values in specified column, does only distinct if necessary
	 * Will round Averages to three places
	 */
	private void doAverage(ArrayList<Row> toFunctionize, int columnIndex, boolean isDistinct) {
		Double totalToAverage = 0.0;
		int divisor = 0;
		HashSet<Double> distinctCheck = new HashSet<Double>();
		if(isDistinct == true) {
			for(Row r:toFunctionize) {
				//This should never throw an exception..unless I have invalid input in table
				if(r.getCell(columnIndex) != null) {
					Double value = ((Number)(r.getCell(columnIndex))).doubleValue();
					distinctCheck.add(value);
				}
			}
			for(Double value:distinctCheck) {
				totalToAverage += value;
				divisor++;
			}
		}
		else {
			for(Row r:toFunctionize) {
				if (r.getCell(columnIndex) != null) {
					totalToAverage += ((Number)(r.getCell(columnIndex))).doubleValue();
				}
			}
			divisor = toFunctionize.size();
		}
		//My averages will always round to three decimal places
		totalToAverage =  Math.floor(((totalToAverage/divisor)*1000))/1000;
		for(Row r: toFunctionize) {
			r.replaceCell(columnIndex, totalToAverage);
		}
	}
	
	/**
	 * Sums values in specified column, does only distinct if necessary
	 */
	private void doSum(ArrayList<Row> toFunctionize, int columnIndex, boolean isDistinct) {
		Double sum = 0.0;
		HashSet<Double> distinctCheck = new HashSet<Double>();
		if(isDistinct == true) {
			for(Row r:toFunctionize) {
				//See doAverage for logic
				if (r.getCell(columnIndex) != null) {
					Double value = ((Number)(r.getCell(columnIndex))).doubleValue();
					distinctCheck.add(value);
				}
			}
			for(Double value:distinctCheck) sum += value;
		}
		else {
			for(Row r:toFunctionize) {
				if(r.getCell(columnIndex) != null) {
					sum += ((Number)(r.getCell(columnIndex))).doubleValue();
				}
			}
		}
		//I return a 'double' sum even for INT rows, which allows sums that are larger than INT.MAX_VALUE
		for(Row r: toFunctionize) {
			r.replaceCell(columnIndex, sum);		
		}
	}
	
	/**
	 * Gets max value in specified column, does only distinct if necessary
	 */
	private void doMax(ArrayList<Row> toFunctionize, int columnIndex, boolean isDistinct) {
		DataType functionDataType = selectedColumnDescriptions.get(columnIndex).getColumnType();
		if(functionDataType == DataType.VARCHAR) {
			doMaxVarchar(toFunctionize, columnIndex, isDistinct);
		}
		else {
			doMaxDoubleAndInt(toFunctionize, columnIndex, isDistinct);
		}
	}
	/**
	 * Gets max value in specified double/int column, does only distinct if necessary
	 */
	private void doMaxDoubleAndInt(ArrayList<Row> toFunctionize, int columnIndex, boolean isDistinct) {
		Double max = Double.MIN_VALUE;
		HashSet<Double> distinctCheck = new HashSet<Double>();
		if(isDistinct == true) {
			for(Row r:toFunctionize) {
				//See doAverage for logic
				if (r.getCell(columnIndex) != null) {
					Double value = ((Number)(r.getCell(columnIndex))).doubleValue();
					distinctCheck.add(value);
				}
			}
			for(Double value:distinctCheck) if(value > max) max = value;
		}
		else {
			for(Row r:toFunctionize) {
				if(r.getCell(columnIndex) != null && ((Number)(r.getCell(columnIndex))).doubleValue() > max) {
					max = ((Number)(r.getCell(columnIndex))).doubleValue();
				}
			}
		}
		//Converting to proper column type and adding to table
		if(selectedColumnDescriptions.get(columnIndex).getColumnType() == DataType.INT) {
			int intMax = max.intValue();
			for(Row r: toFunctionize) r.replaceCell(columnIndex, intMax);
		}
		else {
			for(Row r: toFunctionize) r.replaceCell(columnIndex, max);		
		}
	}
	
	/**
	 * Gets max value in varchar column, does only distinct if necessary
	 */
	private void doMaxVarchar(ArrayList<Row> toFunctionize, int columnIndex, boolean isDistinct) {
		String max = "";
		if(isDistinct == true) {
			HashSet<String> distinctCheck = new HashSet<String>();
			for(Row r:toFunctionize) {
				if (r.getCell(columnIndex) != null) {
					String value = r.getCelltoString(columnIndex);
					distinctCheck.add(value);
				}
			}
			for(String currentCell:distinctCheck) {
				if(currentCell.compareToIgnoreCase(max) > 0) max = currentCell;
			}
		}
		else {
			for(Row r: toFunctionize) {
				if (r.getCell(columnIndex) != null) {
					String currentCell = r.getCelltoString(columnIndex);
					if(currentCell.compareToIgnoreCase(max) > 0) max = currentCell;
				}
			}
		}
		//Now add back to table
		for(Row r: toFunctionize) r.replaceCell(columnIndex, max);
	}
	
	
	/**
	 * Gets min value in specified column, does only distinct if necessary
	 */
	private void doMin(ArrayList<Row> toFunctionize, int columnIndex, boolean isDistinct) {
		DataType functionDataType = selectedColumnDescriptions.get(columnIndex).getColumnType();
		if(functionDataType == DataType.VARCHAR) {
			doMinVarchar(toFunctionize, columnIndex, isDistinct);
		}
		else {
			doMinDoubleAndInt(toFunctionize, columnIndex, isDistinct);
		}
	}
	/**
	 * Gets min value in specified double/int, does only distinct if necessary
	 */
	private void doMinDoubleAndInt(ArrayList<Row> toFunctionize, int columnIndex, boolean isDistinct) {
		Double min = Double.MAX_VALUE;
		HashSet<Double> distinctCheck = new HashSet<Double>();
		if(isDistinct == true) {
			for(Row r:toFunctionize) {
				if (r.getCell(columnIndex) != null) {
					Double value = ((Number)(r.getCell(columnIndex))).doubleValue();
					distinctCheck.add(value);
				}
			}
			for(Double value:distinctCheck) if(value < min) min = value;
		}
		else {
			for(Row r:toFunctionize) {
				if(r.getCell(columnIndex) != null && ((Number)(r.getCell(columnIndex))).doubleValue() < min) {
					min = ((Number)(r.getCell(columnIndex))).doubleValue();
				}
			}
		}
		//Converting to proper column type and adding to table
		if(selectedColumnDescriptions.get(columnIndex).getColumnType() == DataType.INT) {
			int intMin = min.intValue();
			for(Row r: toFunctionize) r.replaceCell(columnIndex, intMin);
		}
		else {
			for(Row r: toFunctionize) r.replaceCell(columnIndex, min);		
		}
	}
	
	/**
	 * Gets min value in varchar column, does only distinct if necessary
	 */
	private void doMinVarchar(ArrayList<Row> toFunctionize, int columnIndex, boolean isDistinct) {
		String min = "ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ";
		if(isDistinct == true) {
			HashSet<String> distinctCheck = new HashSet<String>();
			for(Row r:toFunctionize) {
				if (r.getCell(columnIndex) != null) {
					String value = r.getCelltoString(columnIndex);
					distinctCheck.add(value);
				}
			}
			for(String currentCell:distinctCheck) {
				if(currentCell.compareToIgnoreCase(min) < 0) min = currentCell;
			}
		}
		else {
			for(Row r: toFunctionize) {
				String currentCell = r.getCelltoString(columnIndex);
				if(currentCell.compareToIgnoreCase(min) < 0) min = currentCell;
			}
		}
		//Now add back to table
		for(Row r: toFunctionize) r.replaceCell(columnIndex, min);
	}
	
	/**
	 * Sets what columns are to be selected based on select query
	 * @param columndID
	 */
	private void setSelectedColumnDescriptions(ColumnID[] columndID) {
		if(columndID[0].getColumnName().trim().equals("*")) {
			selectedColumnDescriptions = selectFrom.getColumnDescriptions();
		}
		else {
			for(ColumnID c:columndID) {
				if(allColumnDescriptionsByName.containsKey(c.getColumnName())) {
					selectedColumnDescriptions.add(allColumnDescriptionsByName.get(c.getColumnName()));
				}
				else {
					String error = "Column " + c.getColumnName() + " does not exist";
					throw new IllegalArgumentException(error);
				}
			}			
		}
	}		
	
	/**
	 * Cuts off and orders columns so that we can return only what was in the query (in order)
	 */
	private ArrayList<Row> trimExcessColumns(ArrayList<Row> toTrim) {
		ArrayList<Row> onlySelectedColumns = new ArrayList<Row>();
		int numberOfRows = toTrim.size();
		for(int i = 0;i < numberOfRows;i++) onlySelectedColumns.add(new Row());
		for(int i = 0; i < selectedColumnDescriptions.size();i++) {
			int columnToAdd = columnIndices.get(selectedColumnDescriptions.get(i).getColumnName());
			for(int j = 0; j < toTrim.size(); j++) {
				Object cellToAdd = toTrim.get(j).getCell(columnToAdd);
				onlySelectedColumns.get(j).addCell(cellToAdd);
			}
		}
		return onlySelectedColumns;
	}
	
	/**
	 * Using a bubble sort algorithm (I know, not very efficient),
	 * with help from the tutorial
	 * on algolist.net/Algorithms/Sorting/Bubble_sort
	 * @param toOrder The selected columns that I will be ordering
	 * @param orderByConditions The orderby conditions from the query
	 */
	private void orderBy(ArrayList<Row> toOrder, OrderBy[] orderByConditions) {
		if(orderByConditions == null) {
			return;
		}
		for(int i = orderByConditions.length - 1; i >= 0;i--) {
			if(orderByConditions[i] != null && orderByConditions[i].isAscending() == true) { //To make sure that there are any order by conditions
				orderByAscending(orderByConditions[i], toOrder);			
			}
			else if(orderByConditions[i] != null && orderByConditions[i].isDescending() == true) { //To make sure that there are any order by conditions
					orderByDescending(orderByConditions[i], toOrder);		
			}
		}
	}

	/**
	 * Orders a row by ascending values
	 */
	void orderByAscending(OrderBy orderByConditions, ArrayList<Row> toOrder) {
		int columnIndex = columnIndices.get(orderByConditions.getColumnID().getColumnName());
		boolean swapped = true;
		int j = 0;
		Row temp;
		while (swapped) {
			swapped = false;
			j++;
			for(int k = 0; k < toOrder.size() - j;k++) {
				int firstIsLarger = compareCells(toOrder.get(k).getCell(columnIndex), toOrder.get(k+1).getCell(columnIndex));
				if(firstIsLarger > 0) {
					temp = toOrder.get(k).cloneRow();
					toOrder.set(k, toOrder.get(k+1));
					toOrder.set(k+1, temp);
					swapped = true;
				}
			}
		}
	}
	/**
	 * Orders a row by descending values
	 */
	void orderByDescending(OrderBy orderByConditions, ArrayList<Row> toOrder) {

		if(columnIndices.get(orderByConditions.getColumnID().getColumnName()) == null) {
			throw new IllegalArgumentException("Column " + orderByConditions.getColumnID().getColumnName() + " doesnt exist");
		}
		int columnIndex = columnIndices.get(orderByConditions.getColumnID().getColumnName());
		boolean swapped = true;
		int j = 0;
		Row temp;
		while (swapped) {
			swapped = false;
			j++;
			for(int k = 1; k < toOrder.size() - j;k++) {
				int firstIsLarger = compareCells(toOrder.get(k-1).getCell(columnIndex), toOrder.get(k).getCell(columnIndex));
				if(firstIsLarger < 0) { //If the first is larger is false (less than 0)
					temp = toOrder.get(k-1).cloneRow();
					toOrder.set(k-1, toOrder.get(k));
					toOrder.set(k, temp);
					swapped = true;
				}
			}
		}	
	}
	
	/**
	 * If one > two, result = 1
	 * If one == two, result = 0
	 * If one < two, result = -1
	 * Compares which cell is greater
	 */
	private int compareCells(Object one, Object two) {
		int result = 0;
		try {
			if(one != null && two == null) return 1;
			if(one == null && two == null) return 0;
			if(one == null && two != null) return -1;
			if(one instanceof Boolean) {
				if(((Boolean)one).compareTo((Boolean)two) < 0) result = -1;
				if(((Boolean)one).compareTo((Boolean)two) > 0) result = 1;
			}
			else if(one instanceof String) {
				if(one.toString().compareToIgnoreCase(two.toString()) <= 0) result = -1;
				if(one.toString().compareToIgnoreCase(two.toString()) > 0) result = 1;

			}
			else if(one instanceof Double) {
				if((Double)one < (Double)two) result = -1;
				if((Double)one > (Double)two) result = 1;
			}
			else if(one instanceof Integer){
				if((Integer)one < (Integer)two) result = -1;
				if((Integer)one > (Integer)two) result = 1;
			}
		} catch (Exception e) {
			String error= "Catastrophic error with orderBy - check imediately";//condition.getRightOperand().toString() + " is invalid input for the *where* clause in this column";
			throw new IllegalArgumentException(error);
		}
		return result;
	}
	
	/**
	 * Does wheres and returns whered columns, uses index if all rows are indexed
	 * @param whereCondition
	 * @return
	 */
	private ArrayList<Row> getRowsByWhere(Condition whereCondition) {
		ArrayList<Row> result = new ArrayList<Row>();
		if(whereCondition != null) {
			HelperWhere whereHandler = new HelperWhere(columnIndices);
			boolean onlyHasIndexedColumns = whereHandler.onlyHasIndexedColumns(whereCondition, selectFrom.getIndexedColumns());
			if(onlyHasIndexedColumns) {
				result = whereHandler.checkAlreadyIndexedRows(whereCondition, selectFrom);
			}
			else {
				for(int i = 0; i < selectFrom.getTable().size(); i++) {
					if(whereHandler.checkRowByWhere(whereCondition, selectFrom.getRow(i))) {
						result.add(selectFrom.getRow(i).cloneRow());
					}
				}
			}
		}
		else { //If there is no where condition
			for(int i = 0; i < selectFrom.getTable().size(); i++) {
				result.add(selectFrom.getRow(i).cloneRow());
			}
		}
		return result;//If null, this will make an empty table with column names
	}
}
