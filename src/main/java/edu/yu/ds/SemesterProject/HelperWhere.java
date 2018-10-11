package edu.yu.ds.SemesterProject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.Condition;
import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.Condition.Operator;

class HelperWhere {
	HashMap<String, Integer>  columnIndices;
	
	public HelperWhere(HashMap<String, Integer> columnIndices) {
		this.columnIndices = columnIndices;
	}
	
	/**
	 * If all columns are already indexed, use this to return the rows
	 * (I know its longer than 30 lines but there are a lot of conditions, 
	 * and i made it as clear as possible)
	 */
	ArrayList<Row> checkAlreadyIndexedRows(Condition condition, Table toWhereFrom) {
		ArrayList<Row> passesWhereCheck = new ArrayList<Row>();
		Operator currentOperator = condition.getOperator();
		switch (currentOperator) {
			case AND: {
				ArrayList<Row> leftOperandResult = checkAlreadyIndexedRows((Condition)condition.getLeftOperand(), toWhereFrom);
				ArrayList<Row> rightOperandResult = checkAlreadyIndexedRows((Condition)condition.getRightOperand(), toWhereFrom);

				for(Row leftRow: leftOperandResult) {
					for(Row rightRow:rightOperandResult) {
						if(leftRow.equals(rightRow)) {
							passesWhereCheck.add(rightRow);
						}
					}
				}
			}
				break;
			case OR: {
				ArrayList<Row> leftOperandResult = checkAlreadyIndexedRows((Condition)condition.getLeftOperand(), toWhereFrom);
				ArrayList<Row> rightOperandResult = checkAlreadyIndexedRows((Condition)condition.getRightOperand(), toWhereFrom);
				HashSet<Row> result = new HashSet<Row>();
				result.addAll(leftOperandResult);
				result.addAll(rightOperandResult);
				passesWhereCheck.addAll(result);
			}
				break;
			case GREATER_THAN_OR_EQUALS: {
				String columnName = condition.getLeftOperand().toString();
				String value = condition.getRightOperand().toString();
				ArrayList<Row> result = toWhereFrom.getIndexedColumns().get(columnName).getGreaterThanOrEquals(value);
				passesWhereCheck.addAll(result);
			}
				break;							
			case GREATER_THAN: {
				String columnName = condition.getLeftOperand().toString();
				String value = condition.getRightOperand().toString();
				ArrayList<Row> result = toWhereFrom.getIndexedColumns().get(columnName).getGreaterThan(value);
				passesWhereCheck.addAll(result);
				
			}
				break;
			case LESS_THAN_OR_EQUALS: {
				String columnName = condition.getLeftOperand().toString();
				String value = condition.getRightOperand().toString();
				ArrayList<Row> result = toWhereFrom.getIndexedColumns().get(columnName).getLessThanOrEquals(value);
				passesWhereCheck.addAll(result);
			}
				break;
			case LESS_THAN: {
				String columnName = condition.getLeftOperand().toString();
				String value = condition.getRightOperand().toString();
				ArrayList<Row> result = toWhereFrom.getIndexedColumns().get(columnName).getLessThan(value);
				passesWhereCheck.addAll(result);
				
			}
				break;
			case NOT_EQUALS: {
				String columnName = condition.getLeftOperand().toString();
				String value = condition.getRightOperand().toString();
				ArrayList<Row> result = toWhereFrom.getIndexedColumns().get(columnName).getNotEquals(value);
				passesWhereCheck.addAll(result);
			}
				break;
			case EQUALS: {
				String columnName = condition.getLeftOperand().toString();
				String value = condition.getRightOperand().toString();
				ArrayList<Row> result = toWhereFrom.getIndexedColumns().get(columnName).getEquals(value);
				passesWhereCheck.addAll(result);
			}
				break;
		}
		return passesWhereCheck;
	}
	/**
	 * Returns a list of all of the names of the indexed columns contained inside of the where query
	 */
	ArrayList<String> getIndexedColumns(Condition condition, HashMap<String, BTree<String,ArrayList<Row>>> indexedColumns) {
		ArrayList<String> result = new ArrayList<String>();
		switch (condition.getOperator()) {
		//Left operand is column id, right is value to check
			case AND: {
				ArrayList<String> rightOperandResult = getIndexedColumns((Condition)condition.getRightOperand(), indexedColumns);
				ArrayList<String> leftOperandResult = getIndexedColumns((Condition)condition.getLeftOperand(), indexedColumns);
				result.addAll(rightOperandResult);
				result.addAll(leftOperandResult);
				break;
			}
			case OR: {
				ArrayList<String> rightOperandResult = getIndexedColumns((Condition)condition.getRightOperand(), indexedColumns);
				ArrayList<String> leftOperandResult = getIndexedColumns((Condition)condition.getLeftOperand(), indexedColumns);
				result.addAll(rightOperandResult);
				result.addAll(leftOperandResult);
				break;
			}
			default:
				if(indexedColumns.get(condition.getLeftOperand().toString()) != null) {
					result.add(condition.getLeftOperand().toString());
				}
		}
		return result;		
	}
	
	/**
	 * Finds all columns, and returns true if they are all indexed
	 */
	boolean onlyHasIndexedColumns(Condition condition, HashMap<String, BTree<String,ArrayList<Row>>> indexedColumns) {
		boolean hasExclusivelyIndexedColumns = true;
		switch (condition.getOperator()) {
		//Left operand is column id, right is value to check
			case AND: {
				boolean rightOperandResult = onlyHasIndexedColumns((Condition)condition.getRightOperand(), indexedColumns);
				boolean leftOperandResult = onlyHasIndexedColumns((Condition)condition.getLeftOperand(), indexedColumns);
				if(!rightOperandResult || !leftOperandResult) {
					hasExclusivelyIndexedColumns = false;
				}
				break;
			}
			case OR:{
				boolean rightOperandResult = onlyHasIndexedColumns((Condition)condition.getRightOperand(), indexedColumns);
				boolean leftOperandResult = onlyHasIndexedColumns((Condition)condition.getLeftOperand(), indexedColumns);
				if(!rightOperandResult || !leftOperandResult) {
					hasExclusivelyIndexedColumns = false;
				}
				break;
			}
			default:
				if(indexedColumns.get(condition.getLeftOperand().toString()) == null) {
					hasExclusivelyIndexedColumns = false;
				}
		}
		return hasExclusivelyIndexedColumns;
	}

	/**
	 * Takes in a row as input, and returns true if it meets the criteria of the where
	 */
	boolean checkRowByWhere(Condition condition, Row row) {
		boolean passesWhereCheck = true;
		Operator currentOperator = condition.getOperator();
		switch (currentOperator) {
		//Left operand is column id, right is value to check
			case AND:
				passesWhereCheck = whereAnd(condition, row);
				break;
			case OR:
				passesWhereCheck = whereOr(condition, row);
				break;
			case GREATER_THAN_OR_EQUALS:
				passesWhereCheck = whereGreaterThanOrEquals(condition, row);
				break;							
			case GREATER_THAN:
				passesWhereCheck = whereGreaterThan(condition, row);
				break;
			case LESS_THAN_OR_EQUALS:
				passesWhereCheck = whereLessThanOrEquals(condition, row);
				break;
			case LESS_THAN:
				passesWhereCheck = whereLessThan(condition, row);
				break;
			case NOT_EQUALS:
				passesWhereCheck = whereNotEquals(condition, row);
				break;
			case EQUALS:
				passesWhereCheck = whereEquals(condition, row);
				break;
		}
		return passesWhereCheck;
	}
	/**
	 * If its an and, recursively go through condition tree until we get to a non and
	 */
	boolean whereAnd(Condition condition, Row row) {
		//With an AND, the left and right operands' operator will always be conditions, 
		boolean leftResult = checkRowByWhere((Condition)condition.getLeftOperand(), row);
		boolean rightResult = checkRowByWhere((Condition)condition.getRightOperand(), row);
		if(!rightResult || !leftResult) {
			return false;
		}
		else {
			return true;
		}
	}
	/**
	 * If its an or, recursively go through condition tree until we get to a non or
	 */
	boolean whereOr(Condition condition, Row row) {
		//With an OR, the left and right operands' operator will also always be conditions, 
		boolean leftResult = checkRowByWhere((Condition)condition.getLeftOperand(), row);
		boolean rightResult = checkRowByWhere((Condition)condition.getRightOperand(), row);
		if(!rightResult && !leftResult) {
			return false;
		}
		else {
			return true;
		}
	}
	/**
	 * Return true if the value in the column specified row is greater than or equals
	 * to the value in the where condition
	 */
	boolean whereGreaterThanOrEquals(Condition condition, Row row) {
		try {
			int columnIndex = columnIndices.get(condition.getLeftOperand().toString());
			if(row.getCell(columnIndex) != null && condition.getRightOperand() == null) return true;
			if(row.getCell(columnIndex) == null && condition.getRightOperand() == null) return true;
			if(row.getCell(columnIndex) == null && condition.getRightOperand() != null) return false;
			if((row.getCell(columnIndex)) instanceof Boolean) {
				Boolean toCompare = (Boolean)row.getCell(columnIndex);
				Boolean rightOperand = Boolean.parseBoolean(condition.getRightOperand().toString());
				if(toCompare.compareTo(rightOperand) < 0) {
					return false;
				}
			}
			else if(row.getCell(columnIndex) instanceof String) {
				if(row.getCell(columnIndex).toString().compareToIgnoreCase(condition.getRightOperand().toString()) < 0) {
					return false;
				}
			}
			else if(row.getCell(columnIndex) instanceof Double) {
				if((Double)row.getCell(columnIndex) < Double.parseDouble(condition.getRightOperand().toString())) {
					return false;
				}
			}
			else if((Integer)row.getCell(columnIndex) < Integer.parseInt(condition.getRightOperand().toString())) {
				return false;
			}
		} catch (Exception e) {
			throw new IllegalArgumentException(condition.getRightOperand().toString() + " is invalid input for the *where* clause in this column");
		}
		return true;
	}
	
	/**
	 * Return true if the value in the column specified row is greater than 
	 * the value in the where condition
	 */
	boolean whereGreaterThan(Condition condition, Row row) {
		try {
			String columnName = condition.getLeftOperand().toString();
			int columnIndex = columnIndices.get(columnName);
			if(row.getCell(columnIndex) != null && condition.getRightOperand() == null) return true;
			if(row.getCell(columnIndex) == null && condition.getRightOperand() == null) return false;
			if(row.getCell(columnIndex) == null && condition.getRightOperand() != null) return false;
			if((row.getCell(columnIndex)) instanceof Boolean) {
				Boolean toCompare = (Boolean)row.getCell(columnIndex);
				Boolean rightOperand = Boolean.parseBoolean(condition.getRightOperand().toString());
				if(toCompare.compareTo(rightOperand) <= 0)
					return false;
			}
			else if(row.getCell(columnIndex) instanceof String) {
				if(row.getCell(columnIndex).toString().compareToIgnoreCase(condition.getRightOperand().toString()) <= 0) 
					return false;
			}
			else if(row.getCell(columnIndex) instanceof Double) {
				if((Double)row.getCell(columnIndex) <= Double.parseDouble(condition.getRightOperand().toString()))
					return false;
			}
			else if((Integer)row.getCell(columnIndex) <= Integer.parseInt(condition.getRightOperand().toString())) {
				return false;
			}
		} catch (Exception e) {
			String error= condition.getRightOperand().toString() + " is invalid input for the *where* clause in this column";
			throw new IllegalArgumentException(error);
		}
		return true;
	}
	/**
	 * Return true if the value in the column specified row is less than or equals
	 * to the value in the where condition
	 */
	boolean whereLessThanOrEquals(Condition condition, Row row) {
		try {
			String columnName = condition.getLeftOperand().toString();
			int columnIndex = columnIndices.get(columnName);
			if(row.getCell(columnIndex) != null && condition.getRightOperand() == null) return false;
			if(row.getCell(columnIndex) == null && condition.getRightOperand() == null) return true;
			if(row.getCell(columnIndex) == null && condition.getRightOperand() != null) return true;
			if((row.getCell(columnIndex)) instanceof Boolean) {
				Boolean toCompare = (Boolean)row.getCell(columnIndex);
				Boolean rightOperand = Boolean.parseBoolean(condition.getRightOperand().toString());
				if(toCompare.compareTo(rightOperand) > 0) 
					return false;
			}
			else if(row.getCell(columnIndex) instanceof String) {
				if(row.getCell(columnIndex).toString().compareToIgnoreCase(condition.getRightOperand().toString()) > 0)
					return false;
			}
			else if(row.getCell(columnIndex) instanceof Double) {
				if((Double)row.getCell(columnIndex) == Double.parseDouble(condition.getRightOperand().toString()))
					return false;
			}
			else if((Integer)row.getCell(columnIndex) == Integer.parseInt(condition.getRightOperand().toString())) {
				return false;
			}
		} catch (Exception e) {
			String error = condition.getRightOperand().toString() + " is invalid input for the *where* clause in this column";
			throw new IllegalArgumentException(error);
		}
		return true;
	}
	/**
	 * Return true if the value in the column specified row is less than
	 * the value in the where condition
	 */
	boolean whereLessThan(Condition condition, Row row) {
		try {
			String columnName = condition.getLeftOperand().toString();
			int columnIndex = columnIndices.get(columnName);
			if(row.getCell(columnIndex) != null && condition.getRightOperand() == null) return false;
			if(row.getCell(columnIndex) == null && condition.getRightOperand() == null) return false;
			if(row.getCell(columnIndex) == null && condition.getRightOperand() != null) return true;
			if((row.getCell(columnIndex)) instanceof Boolean) {
				Boolean toCompare = (Boolean)row.getCell(columnIndex);
				Boolean rightOperand = Boolean.parseBoolean(condition.getRightOperand().toString());
				if(toCompare.compareTo(rightOperand) >= 0)
					return false;
			}
			else if(row.getCell(columnIndex) instanceof String) {
				if(row.getCell(columnIndex).toString().compareToIgnoreCase(condition.getRightOperand().toString()) >= 0) 
					return false;
			}
			else if(row.getCell(columnIndex) instanceof Double) {
				if((Double)row.getCell(columnIndex) >= Double.parseDouble(condition.getRightOperand().toString())) 
					return false;
			}
			else if((Integer)row.getCell(columnIndex) >= Integer.parseInt(condition.getRightOperand().toString()))  {
				return false;
			}
		} catch (Exception e) {
			String error = condition.getRightOperand().toString() + " is invalid input for the *where* clause in this column";
			throw new IllegalArgumentException(error);
		}
		return true;
	}
	/**
	 * Return true if the value in the column specified row is not equal
	 * to the value in the where condition
	 */
	boolean whereNotEquals(Condition condition, Row row) {
		try {
			String columnName = condition.getLeftOperand().toString();
			int columnIndex = columnIndices.get(columnName);
			if(row.getCell(columnIndex) != null && condition.getRightOperand() == null) return true;
			if(row.getCell(columnIndex) == null && condition.getRightOperand() == null) return false;
			if(row.getCell(columnIndex) == null && condition.getRightOperand() != null) return true;
			if((row.getCell(columnIndex)) instanceof Boolean) {
				Boolean toCompare = (Boolean)row.getCell(columnIndex);
				Boolean rightOperand = Boolean.parseBoolean(condition.getRightOperand().toString());
				if(toCompare.compareTo(rightOperand) == 0)
					return false;
			}
			else if(row.getCell(columnIndex) instanceof String) {
				if(row.getCell(columnIndex).toString().compareToIgnoreCase(condition.getRightOperand().toString()) == 0)
					return false;
			}
			else if(row.getCell(columnIndex) instanceof Double) {
				if((Double)row.getCell(columnIndex) == Double.parseDouble(condition.getRightOperand().toString()))
					return false;
			}
			else if((Integer)row.getCell(columnIndex) == Integer.parseInt(condition.getRightOperand().toString())) {
				return false;
			}
		} catch (Exception e) {
			String error= condition.getRightOperand().toString() + " is invalid input for the *where* clause in this column";
			throw new IllegalArgumentException(error);
		}
		return true;
	}
	/**
	 * Return true if the value in the column specified row is equal
	 * to the value in the where condition
	 */
	boolean whereEquals(Condition condition, Row row) {
		try {
			String columnName = condition.getLeftOperand().toString();
			int columnIndex = columnIndices.get(columnName);
			if(row.getCell(columnIndex) != null && condition.getRightOperand() == null) return false;
			if(row.getCell(columnIndex) == null && condition.getRightOperand() == null) return true;
			if(row.getCell(columnIndex) == null && condition.getRightOperand() != null) return false;
			if((row.getCell(columnIndex)) instanceof Boolean) {
				Boolean toCompare = (Boolean)row.getCell(columnIndex);
				Boolean rightOperand = Boolean.parseBoolean(condition.getRightOperand().toString());
				if(toCompare.compareTo(rightOperand) != 0)
					return false;
			}
			else if(row.getCell(columnIndex) instanceof String) {
				String checkWithoutQuotes = condition.getRightOperand().toString().substring(1, condition.getRightOperand().toString().length() -1);
				if(row.getCell(columnIndex).toString().compareToIgnoreCase(checkWithoutQuotes) != 0)
					return false;
			}
			else if(row.getCell(columnIndex) instanceof Double) {
				if((Double)row.getCell(columnIndex) != Double.parseDouble(condition.getRightOperand().toString()))
					return false;
			}
			else if((Integer)row.getCell(columnIndex) != Integer.parseInt(condition.getRightOperand().toString())) {
				return false;
			}
		} catch (Exception e) {
			throw new IllegalArgumentException(condition.getRightOperand().toString() + " is invalid input for the *where* clause in this column");
		}
		return true;
	}
}
