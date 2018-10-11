package edu.yu.ds.SemesterProject;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.Before;

import org.junit.Test;
import org.junit.experimental.theories.FromDataPoints;

import edu.yu.cs.dataStructures.fall2016.SimpleSQLParser.ColumnDescription;
import net.sf.jsqlparser.statement.select.Select;

public class DBUnitTest {
	
	private static Run run;
	//private static ResultSet result;
	
	@Before
	public void setRunToEmpty() {
		run = new Run();
	}
	
	
	
	public void populateTestDatabase() {
		String createTable = "CREATE TABLE YCStudent"
				+ "("
				+ " BannerID int,"
	            + " SSNum int UNIQUE,"
	            + " Credits int,"
	            + " FirstName varchar(255),"
	            + " LastName varchar(255) NOT NULL,"
	            + " GPA decimal(1,2) DEFAULT 0.0,"
	            + " CurrentStudent boolean DEFAULT true,"
	            + " Class VARCHAR(255),"
	            + " PRIMARY KEY (BannerID)"
	            + ");";
		run.execute(createTable);
		String insertQuery = ("INSERT INTO YCStudent (Class, CurrentStudent, BannerID, FirstName, LastName, SSNum, GPA, Credits) VALUES ('Junior', false, 806034243, 'Johnny','Depp', 2111234234, 2.2, 45);");
		run.execute(insertQuery);
		insertQuery = ("INSERT INTO YCStudent (Class, CurrentStudent, BannerID, FirstName, LastName, SSNum, GPA, Credits) VALUES ('Senior', true, 800016345, 'Natanel','Esraeilian', 1112345678, 1.5, 12);");
		run.execute(insertQuery);
		insertQuery = ("INSERT INTO YCStudent (Class, CurrentStudent, BannerID, FirstName, LastName, SSNum, GPA, Credits) VALUES ('Junior', true, 80024983, 'Jack','Mendels', 765756, 3.1, 34);");
		run.execute(insertQuery);
		insertQuery = ("INSERT INTO YCStudent (Class, CurrentStudent, BannerID, FirstName, LastName, SSNum, GPA, Credits) VALUES ('Freshman', false, 10038424, 'Joseph','Marks', 876543, 2.8, 65);");
		run.execute(insertQuery);	
		insertQuery = ("INSERT INTO YCStudent (Class, CurrentStudent, BannerID, FirstName, LastName, SSNum, Credits) VALUES ('Sophomore', true, 80038444, 'Moshe','Goldman', 234578, 13);");
		run.execute(insertQuery);
		insertQuery = ("INSERT INTO YCStudent (Class, CurrentStudent, BannerID, FirstName, LastName, SSNum, GPA, Credits) VALUES ('Freshman', true, 80074943, 'Yosef','Fink', 785756, 2.7, 34);");
		run.execute(insertQuery);
		insertQuery = ("INSERT INTO YCStudent (Class, CurrentStudent, BannerID, FirstName, LastName, SSNum, GPA, Credits) VALUES ('Senior', true, 8003844, 'Meir','Frank', 876546, 2.6, 65);");
		run.execute(insertQuery);
		insertQuery = ("INSERT INTO YCStudent (Class, CurrentStudent, BannerID, LastName, SSNum, GPA, Credits) VALUES ('Junior', true, 800012345,'Meyers', 2345674, 1.5, 123);");
		run.execute(insertQuery);
		insertQuery = ("INSERT INTO YCStudent (Class, CurrentStudent, BannerID, FirstName, LastName, SSNum, GPA, Credits) VALUES ('Freshman', false, 80074983, 'Eli','Feld', 765752, 2.2, 34);");
		run.execute(insertQuery);
		insertQuery = ("INSERT INTO YCStudent (Class, CurrentStudent, BannerID, FirstName, LastName, SSNum, GPA, Credits) VALUES ('Freshman', true, 80038424, 'Yoav','Green', 87654, 2.1, 65);");
		run.execute(insertQuery);
		
		String indexQuery = "CREATE INDEX GPA_Index on YCStudent (GPA);";
		
		run.execute(indexQuery);
		
		
	}
	
	/*
	 * CreateTable Tests
	 */

	@Test
	public void createTableVanilla() {
		String createTable = "CREATE TABLE YCStudent"
			+ "("
			+ " BannerID int,"
            + " SSNum int UNIQUE,"
            + " Credits int,"
            + " FirstName varchar(255),"
            + " LastName varchar(255) NOT NULL,"
            + " GPA decimal(1,2) DEFAULT 0.0,"
            + " CurrentStudent boolean DEFAULT true,"
            + " Class VARCHAR(255),"
            + " PRIMARY KEY (BannerID)"
            + ");";
		
		//run.execute(createTable).printResultSet();
		ResultSet result = run.execute(createTable);
		assertEquals("No Exceptions Were Thrown", result.getError());
		assertEquals(8, result.getColumnNames().size());
		ArrayList<String> columnNames = new ArrayList<String>();
		columnNames.add("BannerID");
		columnNames.add("SSNum");
		columnNames.add("Credits");
		columnNames.add("FirstName");
		columnNames.add("LastName");
		columnNames.add("GPA");
		columnNames.add("CurrentStudent");
		columnNames.add("Class");
		for(String cd:result.getColumnNames()) {
			assertEquals(true,columnNames.contains(cd));
		}
	}
	@Test
	public void createTablePrimaryHasDefault() {
		String createTable = "CREATE TABLE YCStudent"
			+ "("
			+ " BannerID int DEFAULT 1 ,"
            + " SSNum int UNIQUE,"
            + " FirstName varchar(255),"
            + " LastName varchar(255) NOT NULL,"
            + " GPA decimal(1,2) DEFAULT 0.0,"
            + " CurrentStudent boolean DEFAULT true,"
            + " Class VARCHAR(255),"
            + " PRIMARY KEY (BannerID)"
            + ");";
		ResultSet result = run.execute(createTable);
		assertEquals("Primary Key column can't have a default value", result.getError());
	}
	@Test
	public void createTableBadDefaultValueInt() {
		String createTable = "CREATE TABLE YCStudent"
			+ "("
			+ " BannerID int ,"
            + " SSNum int DEFAULT 5.4 ,"
            + " PRIMARY KEY (BannerID)"
            + ");";
		ResultSet result = run.execute(createTable);
		assertEquals("Bad input for defalut value of SSNum", result.getError());
	}
	@Test
	public void createTableBadDefaultValueBool() {
		String createTable = "CREATE TABLE YCStudent"
			+ "("
			+ " BannerID int ,"
            + " SSNum boolean DEFAULT ojif ,"
            + " PRIMARY KEY (BannerID)"
            + ");";
		ResultSet result = run.execute(createTable);
		assertEquals("Bad input for defalut value of SSNum", result.getError());
	}
	@Test
	public void createTableBadDefaultValueDouble() {
		String createTable = "CREATE TABLE YCStudent"
			+ "("
			+ " BannerID int ,"
            + " Natanel decimal DEFAULT ojif ,"
            + " PRIMARY KEY (BannerID)"
            + ");";
		ResultSet result = run.execute(createTable);
		assertEquals("Bad input for defalut value of Natanel", result.getError());
	}
	
	@Test 
	public void indexTest() {
		populateTestDatabase();
		String indexQuery = "CREATE INDEX GPA_Index on YCStudent (GPA);";
		ResultSet result = run.execute(indexQuery);
		assertEquals(true, result.resultSet.getRow(0).getCell(0));
		
		run.execute("DELETE FROM YCStudent WHERE GPA < 2").getResult();//Uses Index, all columns in "where" are indexed
		Table withIndexDelete = run.execute("SELECT * FROM YCStudent").getResult();
		populateTestDatabase();
		run.execute("DELETE FROM YCStudent WHERE GPA < 2 OR Credits < 0").getResult();//Returns the same answer, but not indexed
		Table withoutIndex = run.execute("SELECT * FROM YCStudent").getResult();
		assertEquals(withoutIndex, withIndexDelete);
		//This means that the index works, because I get the same result when deleting with and without it
	}
	
	
	
	/*
	 * Insert Tests
	 */
	
	@Test
	public void insertTests() {
		String createTable = "CREATE TABLE YCStudent"
				+ "("
				+ " BannerID int,"
	            + " SSNum int UNIQUE,"
	            + " FirstName varchar(20),"
	            + " LastName varchar(20) NOT NULL,"
	            + " GPA decimal(2,4) DEFAULT 0.00,"
	            + " CurrentStudent boolean DEFAULT true,"
	            + " Class VARCHAR(255),"
	            + " PRIMARY KEY (BannerID)"
	            + ");";
			run.execute(createTable);
		String insertQuery = ("INSERT INTO YCStudent (Class, CurrentStudent, BannerID, FirstName, LastName, SSNum, GPA) VALUES ('Senior', true, 800343343, 'Natanel','Esraeilian', 2345678, 1);");
		assertEquals("Vanilla","No Exceptions Were Thrown", run.execute(insertQuery).getError());
		insertQuery = ("INSERT INTO YCStudent (Class, CurrentStudent, FirstName, LastName, SSNum, GPA) VALUES ('Senior', true, 'Natanel','Esraeilian', 2345678, 1);");
		assertEquals("NoPrimaryKey", "Every Line Must Contain a Primary Key", run.execute(insertQuery).getError());
		insertQuery = ("INSERT INTO YCStudent (BannerID, CurrentStudent, FirstName, LastName, SSNum, oijnoin) VALUES ('Senior', true, 'Natanel','Esraeilian', 23678, 1);");
		assertEquals("ColumnDoesntExist", "Column *oijnoin* does not exist in table YCStudent", run.execute(insertQuery).getError());
		insertQuery = ("INSERT INTO YCStudent (Class, CurrentStudent, BannerID, FirstName, SSNum, GPA) VALUES ('Senior', true, 8003343, 'Natanel', 2345678, 1);");
		assertEquals("CheckNotNull","Column *LastName* in table *YCStudent* requires input", run.execute(insertQuery).getError());
		insertQuery = ("INSERT INTO YCStudent (Class, CurrentStudent, BannerID, FirstName, LastName, SSNum, GPA) VALUES ('Senior', true, 800343343, 'Natanel','Esraeilian', 2345678, 1);");
		assertEquals("Duplicates","You entered *800343343*. Column *BannerID* in table *YCStudent* can't have duplicate input. ", run.execute(insertQuery).getError());
		insertQuery = ("INSERT INTO YCStudent (Class, CurrentStudent, BannerID, FirstName, LastName, SSNum, GPA) VALUES ('Senior', true, 8009343, 'Natanel','erger', 23435678, s);");
		assertEquals("Vanilla","You entered *s*. Input to column *GPA* in table *YCStudent* must be a decimal number", run.execute(insertQuery).getError());
	}
	
	@Test 
	public void updateTest() {
		populateTestDatabase();
		String updateQuery = "UPDATE YCStudent SET GPA=3.0,Class='Super Senior' WHERE FirstName = Natanel AND Pizza = 'Junior';";
		ResultSet result = run.execute(updateQuery);
		assertEquals(false, result.resultSet.getRow(0).getCell(0));//Because one of the columns doesn't exist
		updateQuery = "UPDATE YCStudent SET GPA=3.0,Class='Super Senior' WHERE CurrentStudent = true;";
		result = run.execute(updateQuery);
		assertEquals(true, result.resultSet.getRow(0).getCell(0));//Because one of the columns doesn't exist		
	}
	
	@Test
	public void deleteTest() {
		populateTestDatabase();
		String deleteQuery = "DELETE FROM YCStudent;";
		ResultSet resultSet = run.execute(deleteQuery);
		assertEquals(true, resultSet.getResult().getTable().get(0).getCell(0));
		assertEquals(0, run.execute("SELECT * FROM YCStudent").getResult().getTable().size());
		deleteQuery = "DELETE FROM YCStudent WHERE (Class = 'Junior' OR Class = 'Freshman') AND Credits > 50;";
		populateTestDatabase();
		run.execute(deleteQuery);	
		assertEquals(7, run.execute("SELECT * FROM YCStudent").getResult().getTable().size());
	}
	
	@Test
	public void badSqlInput() {
		String insertQuery = "DELETE FROM YCStudent WHEewRE Class='Junior' AND GPA < 3.0;";
		assertEquals(false, run.execute(insertQuery).getResult().getTable().get(0).getCell(0));
	}
	
	
	/*
	 * SelectTests
	 */
	
	@Test
	public void selectTestVanilla() {
		populateTestDatabase();
		String selectQuery = "SELECT * FROM YCStudent;";
		run.execute(selectQuery).printResultSet();
		selectQuery  = "SELECT * FROM YCStudent WHERE LastName < FirstName;";
		run.execute(selectQuery).printResultSet();
		selectQuery  = "SELECT DISTINCT MAX(Distinct GPA), GPA, SUM(SSNum) FROM YCStudent ORDER BY LastName;";
		run.execute(selectQuery).printResultSet();
		selectQuery = "Select DISTINCT FirstName, LastName FROM YCStudent WHERE GPA >= 2.4 And (CurrentStudent = true OR Class = Senior);";
		run.execute(selectQuery).printResultSet();
		selectQuery = "SELECT * FROM YCStudent ORDERBY GPA ASC;";
		run.execute(selectQuery).printResultSet();
		selectQuery = "SELECT * FROM YCStudent ORDERBY GPA ASC, Credits DESC;";
		run.execute(selectQuery).printResultSet();
		
	}
	
	
	@Test
	public void selectTestDouble() {
		populateTestDatabase();
		String selectQuery = "SELECT AVG(GPA) FROM YCStudent;";
		assertEquals(2.07, run.execute(selectQuery).getResult().getTable().get(0).getCell(0));
		selectQuery = "SELECT COUNT(GPA) FROM YCStudent;";
		run.execute(selectQuery);
		assertEquals(10, run.execute(selectQuery).getResult().getTable().get(0).getCell(0));
		selectQuery = "SELECT COUNT(DISTINCT GPA) FROM YCStudent;";
		run.execute(selectQuery);
		assertEquals(8, run.execute(selectQuery).getResult().getTable().get(0).getCell(0));
		selectQuery = "SELECT MAX(GPA) FROM YCStudent;";
		run.execute(selectQuery);
		assertEquals(3.1, run.execute(selectQuery).getResult().getTable().get(0).getCell(0));
		selectQuery = "SELECT MIN(GPA) FROM YCStudent;";
		run.execute(selectQuery);
		assertEquals(0.0, run.execute(selectQuery).getResult().getTable().get(0).getCell(0));
		selectQuery = "SELECT SUM(GPA) FROM YCStudent;";
		run.execute(selectQuery);
		assertEquals(20.7, run.execute(selectQuery).getResult().getTable().get(0).getCell(0));		
	}
	@Test
	public void selectTestInt() {
		populateTestDatabase();
		String selectQuery = "SELECT AVG(Credits) FROM YCStudent;";
		assertEquals(49.0, run.execute(selectQuery).getResult().getTable().get(0).getCell(0));
		selectQuery = "SELECT COUNT(Credits) FROM YCStudent;";
		run.execute(selectQuery);
		assertEquals(10, run.execute(selectQuery).getResult().getTable().get(0).getCell(0));
		selectQuery = "SELECT COUNT(DISTINCT Credits) FROM YCStudent;";
		run.execute(selectQuery);
		assertEquals(6, run.execute(selectQuery).getResult().getTable().get(0).getCell(0));
		selectQuery = "SELECT MAX(Credits) FROM YCStudent;";
		run.execute(selectQuery);
		assertEquals(123, run.execute(selectQuery).getResult().getTable().get(0).getCell(0));
		selectQuery = "SELECT MIN(Credits) FROM YCStudent;";
		run.execute(selectQuery);
		assertEquals(12, run.execute(selectQuery).getResult().getTable().get(0).getCell(0));
		selectQuery = "SELECT SUM(Credits) FROM YCStudent;";
		run.execute(selectQuery);
		assertEquals(490.0, run.execute(selectQuery).getResult().getTable().get(0).getCell(0));		
	}
	@Test
	public void selectTestVarchar() {
		populateTestDatabase();
		String selectQuery = "SELECT COUNT(Class) FROM YCStudent;";
		assertEquals(10, run.execute(selectQuery).getResult().getTable().get(0).getCell(0));
		selectQuery = "SELECT COUNT(DISTINCT Class) FROM YCStudent;";
		assertEquals(4, run.execute(selectQuery).getResult().getTable().get(0).getCell(0));
		selectQuery = "SELECT MAX(Class) FROM YCStudent;";
		run.execute(selectQuery).printResultSet();
		assertEquals("Sophomore", run.execute(selectQuery).getResult().getTable().get(0).getCell(0));
		selectQuery = "SELECT MIN(Class) FROM YCStudent;";
		assertEquals("Freshman", run.execute(selectQuery).getResult().getTable().get(0).getCell(0));
	}
	@Test
	public void selectTestBool() {
		populateTestDatabase();
		String selectQuery = "SELECT COUNT(CurrentStudent) FROM YCStudent;";
		run.execute(selectQuery);
		assertEquals(10, run.execute(selectQuery).getResult().getTable().get(0).getCell(0));
		selectQuery = "SELECT COUNT(DISTINCT CurrentStudent) FROM YCStudent;";
		run.execute(selectQuery);
		assertEquals(2, run.execute(selectQuery).getResult().getTable().get(0).getCell(0));
	}
	
	
	@Test
	public void selectWhereTest() {
		populateTestDatabase();
		String selectQuery = "SELECT FirstName, LastName FROM YCStudent WHERE Credits > 3;";
		run.execute(selectQuery).printResultSet();
		selectQuery = "SELECT FirstName, LastName FROM YCStudent WHERE Credits < 25 AND CurrentStudent = TRUE;";
		run.execute(selectQuery).printResultSet();
		selectQuery = "SELECT FirstName, LastName FROM YCStudent WHERE Credits = 50;";
		run.execute(selectQuery).printResultSet();
		selectQuery = "SELECT FirstName, LastName FROM YCStudent WHERE Credits < 100;";
		run.execute(selectQuery).printResultSet();
		selectQuery = "SELECT FirstName, LastName FROM YCStudent WHERE Credits >= 67;";
		run.execute(selectQuery).printResultSet();
		selectQuery = "SELECT FirstName, LastName FROM YCStudent WHERE Credits <= 34;";
		run.execute(selectQuery).printResultSet();
		selectQuery = "SELECT FirstName, LastName FROM YCStudent WHERE Credits <> 38;";
		run.execute(selectQuery).printResultSet();		
	}
	
	
	//I tested orderbys by printing it out to the screen and checking it. See DBTest

}
