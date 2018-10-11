package edu.yu.ds.SemesterProject;

public class DBTest {
	
	public static void main(String[] Args) {
		Run run = new Run();
		createTableTest(run);
		insertTest(run);
		indexTest(run);
		selectTest(run);
		selectFunctions(run);
		updateTest(run);
		deleteTest(run);
	}
	
	public static void createTableTest(Run run) {
		String createTable = "CREATE TABLE YCStudent"
				+ "("
				+ " BannerID int,"
	            + " SSNum int UNIQUE,"
	            + " Credits int,"
	            + " FirstName varchar(255),"
	            + " LastName varchar(255) NOT NULL,"
	            + " GPA decimal(1,2),"
	            + " CurrentStudent boolean DEFAULT true,"
	            + " Class VARCHAR(255),"
	            + " PRIMARY KEY (BannerID)"
	            + ");";
		System.out.println(createTable);
		ResultSet result = run.execute(createTable);
		result.printResultSet();
	}
		
	private static void insertTest(Run run) {
		String insertQuery = ("INSERT INTO YCStudent (Class, CurrentStudent, BannerID, FirstName, LastName, SSNum, GPA, Credits) VALUES ('Junior', false, 806034243, 'Johnny','Depp', 2111234234, 2.2, 45);");
		System.out.println(insertQuery);
		run.execute(insertQuery).printResultSet();
		insertQuery = ("INSERT INTO YCStudent (Class, CurrentStudent, BannerID, FirstName, LastName, SSNum, GPA, Credits) VALUES ('Senior', true, 800016345, 'Natanel','Esraeilian', 1112345678, 1.5, 12);");
		System.out.println(insertQuery);
		run.execute(insertQuery).printResultSet();
		insertQuery = ("INSERT INTO YCStudent (Class, CurrentStudent, BannerID, FirstName, LastName, SSNum, GPA, Credits) VALUES ('Junior', true, 80024983, 'Jack','Mendels', 765756, 3.1, 34);");
		System.out.println(insertQuery);
		run.execute(insertQuery).printResultSet();		
		insertQuery = ("INSERT INTO YCStudent (Class, CurrentStudent, BannerID, FirstName, LastName, SSNum, GPA, Credits) VALUES ('Freshman', false, 10038424, 'Joseph','Marks', 876543, 2.8, 65);");
		run.execute(insertQuery);	
		insertQuery = ("INSERT INTO YCStudent (Class, CurrentStudent, BannerID, FirstName, LastName, SSNum, Credits) VALUES ('Sophomore', true, 80038444, 'Moshe','Goldman', 234578, 13);");
		System.out.println(insertQuery);
		run.execute(insertQuery).printResultSet();		
		insertQuery = ("INSERT INTO YCStudent (Class, CurrentStudent, BannerID, FirstName, LastName, SSNum, GPA, Credits) VALUES ('Freshman', true, 80074943, 'Yosef','Fink', 785756, 2.7, 34);");
		System.out.println(insertQuery);
		run.execute(insertQuery).printResultSet();		
		insertQuery = ("INSERT INTO YCStudent (Class, CurrentStudent, BannerID, FirstName, LastName, SSNum, GPA, Credits) VALUES ('Senior', true, 8003844, 'Meir','Frank', 876546, 2.6, 65);");
		run.execute(insertQuery);
		insertQuery = ("INSERT INTO YCStudent (Class, CurrentStudent, BannerID, LastName, SSNum, GPA, Credits) VALUES ('Junior', true, 800012345,'Meyers', 2345674, 1.5, 123);");
		System.out.println(insertQuery);
		run.execute(insertQuery).printResultSet();		
		insertQuery = ("INSERT INTO YCStudent (Class, CurrentStudent, BannerID, FirstName, LastName, SSNum, GPA, Credits) VALUES ('Freshman', false, 80074983, 'Eli','Feld', 765752, 2.2, 34);");
		System.out.println(insertQuery);
		run.execute(insertQuery).printResultSet();		
		insertQuery = ("INSERT INTO YCStudent (Class, CurrentStudent, BannerID, FirstName, LastName, SSNum, GPA, Credits) VALUES ('Freshman', true, 80038424, 'Yoav','Green', 87654, 2.0, 65);");
		System.out.println(insertQuery);
		run.execute(insertQuery).printResultSet();       
	}
	
	private static void indexTest(Run run) {
		String indexQuery = ("CREATE INDEX GPA_Index on YCStudent (GPA);");
		System.out.println(indexQuery);
		run.execute(indexQuery).printResultSet();
	}
	
	private static void selectTest(Run run) {
		String selectQuery = ("SELECT * FROM YCStudent;");
		System.out.println(selectQuery);
		run.execute(selectQuery).printResultSet();
		selectQuery = ("SELECT Class, Credits, LastName FROM YCStudent ORDER BY Credits;");
		System.out.println(selectQuery);
		run.execute(selectQuery).printResultSet();
		selectQuery = ("SELECT Class, Credits, LastName FROM YCStudent WHERE CurrentStudent = true;");
		System.out.println(selectQuery);
		run.execute(selectQuery).printResultSet();
		selectQuery = ("SELECT DISTINCT Class, CurrentStudent FROM YCStudent;");
		System.out.println(selectQuery);
		run.execute(selectQuery).printResultSet();
		selectQuery = ("SELECT DISTINCT LastName, Class, CurrentStudent FROM YCStudent WHERE CurrentStudent = true AND (GPA <= 2 OR LastName = Mendels);");
		System.out.println(selectQuery);
		run.execute(selectQuery).printResultSet();
		selectQuery = ("SELECT DISTINCT LastName, Class, CurrentStudent FROM YCStudent WHERE CurrentStudent <> true AND (GPA >= 2 OR Credits > 50) OR Credits < 20;");
		System.out.println(selectQuery);
		run.execute(selectQuery).printResultSet();
		selectQuery  = "SELECT * FROM YCStudent ORDER BY CurrentStudent ASC, GPA DESC;";
		System.out.println(selectQuery);
		run.execute(selectQuery).printResultSet();
	}
	
	private static void selectFunctions(Run run) {
		String selectQuery = "SELECT AVG(GPA) FROM YCStudent;";
		System.out.println(selectQuery);
		run.execute(selectQuery).printResultSet();
		selectQuery = "SELECT COUNT(CurrentStudent) FROM YCStudent;";
		System.out.println(selectQuery);
		run.execute(selectQuery).printResultSet();
		selectQuery = "SELECT COUNT(DISTINCT Class) FROM YCStudent;";
		System.out.println(selectQuery);
		run.execute(selectQuery).printResultSet();
		selectQuery = "SELECT MAX(Credits) FROM YCStudent;";
		System.out.println(selectQuery);
		run.execute(selectQuery).printResultSet();
		selectQuery = "SELECT MIN(SSNum) FROM YCStudent;";
		System.out.println(selectQuery);
		run.execute(selectQuery).printResultSet();
		selectQuery = "SELECT SUM(Credits) FROM YCStudent;";
		System.out.println(selectQuery);
		run.execute(selectQuery).printResultSet();		
	}
	
	private static void updateTest(Run run) {
		System.out.println("\n\n\n**START UPDATE TEST***");
		System.out.println("Table Before Update");
		run.execute("Select * FROM YCStudent;").printResultSet();
		String updateQuery = "UPDATE YCStudent SET GPA=2.0 WHERE Class = 'Junior';";
		System.out.println(updateQuery);
		run.execute(updateQuery).printResultSet();
		System.out.println("Table After Update #1");
		run.execute("Select * FROM YCStudent;").printResultSet();
		updateQuery = "UPDATE YCStudent SET GPA=3.0,Class='Super Senior' WHERE Class = 'Senior';";
		System.out.println(updateQuery);
		run.execute(updateQuery).printResultSet();
		System.out.println("Table After Update #2");
		run.execute("Select * FROM YCStudent;").printResultSet();
		System.out.println("**END UPDATE TEST*** \n\n\n");
	}
	
	private static void deleteTest(Run run) {
		System.out.println("\n\n\n**START DELETE TEST***");
		System.out.println("Table Before Delete");
		run.execute("Select * FROM YCStudent;").printResultSet();
		String deleteQuery = "DELETE FROM YCStudent WHERE (Class = 'Junior' OR Class = 'Freshman') AND Credits > 50;";
		System.out.println(deleteQuery);
		run.execute(deleteQuery).printResultSet();
		System.out.println("Table After Delete #1");
		run.execute("Select * FROM YCStudent;").printResultSet();
		deleteQuery = "DELETE FROM YCStudent;";
		System.out.println(deleteQuery);
		run.execute(deleteQuery).printResultSet();
		System.out.println("Table After Delete #2");
		run.execute("SELECT * FROM YCStudent;").printResultSet();
		System.out.println("**END DELETE TEST*** \n\n\n");
	}
	
	
	
	
	

}
