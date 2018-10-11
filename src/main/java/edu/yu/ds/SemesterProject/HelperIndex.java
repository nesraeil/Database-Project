package edu.yu.ds.SemesterProject;

import java.util.ArrayList;

class HelperIndex {
	private Table toIndexFrom;	//The table to index from
	
	/**
	 * Sets table to index from the query
	 */
	public HelperIndex(Table toIndexFrom) {
		this.toIndexFrom = toIndexFrom;
	}
	
	/**
	 * Creates a new btree index with the specified column name
	 */
	void createIndex(String columnName) {
		BTree<String,ArrayList<Row>> indexedColumn = insertColumnIntoIndex(toIndexFrom, columnName);
		toIndexFrom.addIndexedRow(indexedColumn, columnName);
	}
	
	/**
	 * Returns the newly made column with the specified column name
	 */
	private BTree<String,ArrayList<Row>> insertColumnIntoIndex(Table toIndexFrom, String columnName) {
		int columnIndex = toIndexFrom.getcolumnIndex().get(columnName);
		//Ideally, I wouldn't want to use strings as the keys, rather the cells
		//But I implemented cell values as objects, which aren't comparable so I am using strings instead.
		BTree<String,ArrayList<Row>> bTreeIndex = new BTree<String, ArrayList<Row>>(); 
		for(Row r:toIndexFrom.getTable()) {
			if (r.getCell(columnIndex) != null) {
				String key = r.getCell(columnIndex).toString();
				//If key is already in the btree, add it to the list of rows at that key
				if(bTreeIndex.get(key) != null) {
					bTreeIndex.get(key).add(r);
				}
				else {
					ArrayList<Row> newlyIndexedRow = new ArrayList<Row>();
					newlyIndexedRow.add(r);
					bTreeIndex.put(key, newlyIndexedRow);
				}
			}
		}
		return bTreeIndex;
	}
}
