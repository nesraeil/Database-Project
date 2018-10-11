package edu.yu.ds.SemesterProject;

import java.util.ArrayList;
import java.util.HashMap;

import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.DeleteQuery;

public class HelperDelete {
	private Table toDeleteFrom; //Table to delete from
	private HashMap<String, Integer> columnIndices; //Gets column indices by string

	/**
	 * Sets fields
	 * @param toDelete Table specified in query
	 */
	HelperDelete(Table toDelete) {
		this.toDeleteFrom = toDelete;
		columnIndices = toDelete.getcolumnIndex();
	}
	
	/**
	 * Logic of the delete helper
	 * @param query Delete Query
	 */
	void deleteRows(DeleteQuery query) {
		if(query.getWhereCondition() != null) { //Do checks if there is a where condition
			HelperWhere whereHandler = new HelperWhere(columnIndices);
 			ArrayList<Row> deleteResult = new ArrayList<Row>();
 			boolean onlyHasIndexedColumns = whereHandler.onlyHasIndexedColumns(query.getWhereCondition(), toDeleteFrom.getIndexedColumns());
			if(onlyHasIndexedColumns) {//If all columns are indexed do this
				deleteOnlyHasIndexedColumns(deleteResult, whereHandler, query);
			}
			else {//Do this if there are non indexed columns in where
				deleteHasNonIndexedColumns(deleteResult, whereHandler, query);
			}
		}
		else { // If no where condition, delete all
			toDeleteFrom.deleteAllRows();
		}
	}
	
	/**
	 * Use this if all columns in the delete where condition are indexed
	 */
	void deleteOnlyHasIndexedColumns(ArrayList<Row> deleteResult, HelperWhere whereHandler, DeleteQuery query) {
		deleteResult = whereHandler.checkAlreadyIndexedRows(query.getWhereCondition(), toDeleteFrom);
		for(int i = 0; i < deleteResult.size();i++) { //If I were doing this again, I would implement a whereIndex method to return the non-deleted values instead of the deleted ones
			toDeleteFrom.deleteRow(deleteResult.get(i));
			//deleting row from the index and table
			for(String indexedColumn:whereHandler.getIndexedColumns(query.getWhereCondition(), toDeleteFrom.getIndexedColumns())) {
				toDeleteFrom.getIndexedColumns().get(indexedColumn).deleteSpecificRow(deleteResult.get(i).getCelltoString(columnIndices.get(indexedColumn)), deleteResult.get(i));
			}
		}
	}
	/**
	 *Use this if not all columns in the delete where condition are indexed
	 */
	void deleteHasNonIndexedColumns(ArrayList<Row> deleteResult, HelperWhere whereHandler, DeleteQuery query) {
		for(int i = 0; i < toDeleteFrom.getTable().size(); i++) {
			if(!whereHandler.checkRowByWhere(query.getWhereCondition(), toDeleteFrom.getTable().get(i))) {
				deleteResult.add(toDeleteFrom.getRow(i));
			}
		}
		for(Row row:deleteResult) {
			if(!toDeleteFrom.getTable().contains(row)) {
				int i = 0;
				for(String indexedColumn:whereHandler.getIndexedColumns(query.getWhereCondition(), toDeleteFrom.getIndexedColumns())) {
					toDeleteFrom.getIndexedColumns().get(indexedColumn).deleteSpecificRow(deleteResult.get(i).getCelltoString(columnIndices.get(indexedColumn)), deleteResult.get(i));
					i++;
				}
			}
		}
		toDeleteFrom.deleteAllRows();
		toDeleteFrom.getTable().addAll(deleteResult);
	}
}
