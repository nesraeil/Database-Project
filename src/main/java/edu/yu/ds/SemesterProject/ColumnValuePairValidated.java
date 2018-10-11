package edu.yu.ds.SemesterProject;

import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.ColumnID;

/**
 * represents the ID of a column and the value to set it to in the INSERT query once
 * the data has been validated
 * Based on ColumnValuePair by diament@yu.edu
 * @author nesraeil@mail.yu.edu
 *
 */

class ColumnValuePairValidated {
	
	private ColumnID col;
    private Object value;

    ColumnValuePairValidated(ColumnID col, Object value)
    {
	this.col = col;
	this.value = value;
    }

    ColumnID getColumnID()
    {
	return this.col;
    }

    Object getValue()
    {
	return this.value;
    }

}
