package edu.yu.ds.SemesterProject;

import java.util.ArrayList;

class Row {
	
	private ArrayList<Object> row; //A list which holds the data in the table
	
	/**
	 * Makes a new empty row
	 */
	Row() {
		this.row = new ArrayList<Object>();				
	}
	
	/**
	 * Makes a new row and adds in one value
	 * For use in ResultSet
	 */
	Row(Object c) {
		this();
		this.addCell(c);
	}
	
	/**
	 * Adds specified value into a row
	 * @param cell
	 */
	void addCell(Object cell) {
		row.add(cell);		
	}

	/**
	 * Gets cell value at specified row index, as a string
	 * @param index Where the value to get is
	 * @return
	 */
	String getCelltoString(int index) {
		if (row.get(index) == null) {
			return "null";
		}
		else {
			return row.get(index).toString();
		}
	}
	
	/**
	 * Gets cell value at specified row index
	 * @param index Where the value to get is
	 * @return
	 */
	Object getCell(int index) {
		return row.get(index);		
	}
	
	/**
	 * Replaces a cell at a given index
	 * @param index - of value to Replace 
	 * @param value - The cell to replace it with
	 */
	void replaceCell(int index, Object value) {
		row.set(index, value);
	}
	
	/**
	 * @return The size of the row
	 */
	int size() {
		return row.size();
	}
	
	/**
	 * Makes a copy of the row, with all new references 
	 * (since all of the wrapper classes are immutable, I dont have to make new ones of those)
	 * @return The newly cloned row
	 */
	Row cloneRow() {
		Row copy = new Row();
		for(Object cell:this.row) {
			copy.addCell(cell);
		}
		return copy;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((row == null) ? 0 : row.hashCode());
		return result;
	}

	/**
	 * My equals goes based on if the rows hold the same elements in the same order
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Row other = (Row) obj;
		if (row == null) {
			if (other.row != null)
				return false;
		} else if (!row.equals(other.row))
			return false;
		return true;
	}
	
	
	
}
