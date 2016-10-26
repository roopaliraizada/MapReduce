package hive;

import hive.udf.ComplexUDFExample;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class HiveJdbcClientExample {
	/*
	 * 
	 * Before Running this example we should start thrift server. To Start
	 * Thrift server we should run below command in terminal hive --service
	 * hiveserver &
	 */
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	static Connection con = null;
	static Statement stmt = null;
	static ResultSet res = null;

	/*
	 * Obtain the connection from the driver and get the instance of
	 * <code>Statement</code>
	 */
	private static void getStatement(String driverName) {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}

		try {
			con = DriverManager.getConnection(
					"jdbc:hive2://localhost:10000/default", "rraizada",
					"impetus123");

			stmt = con.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/*
	 * Register UDF
	 */
	private static void registerUDF(Statement stmt, String udfName, String udfclassName) throws SQLException {
		// TODO Get the queries from a .hiverc file
		String[] udfArgs = new String[2];
		udfArgs[0] = "ADD JAR /Users/rraizada/Documents/eclipse_workspaces/MRP/MapReduce/target/MapReduce-0.0.1-SNAPSHOT.jar";

		udfArgs[1] = "CREATE TEMPORARY FUNCTION " + udfName + " as '" + udfclassName +"'";
		for (String query : udfArgs) {
			stmt.execute(query);
		}
	}

	/*
	 * Executing various Hive operations
	 */
	public static void main(String[] args) throws SQLException {
		try {

			// Obtain the statement
			getStatement(driverName);

			String tableName = "orders";

			// Drops the table
			int result = stmt.executeUpdate("drop table " + tableName);
			System.out.println("Returned " + result + ": Table dropped!");

			// Create table
			result = stmt
					.executeUpdate("create table "
							+ tableName
							+ " (id int, name string, details string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'");
			System.out.println("Returned " + result + ": Table created!");

			// Show tables
			String sql = "show tables"; // '" + tableName + "'";
			System.out.println("Running: " + sql);
			res = stmt.executeQuery(sql);
			if (res.next()) {
				System.out.println(res.getString(1));
			}

			// Describe table
			sql = "describe " + tableName;
			System.out.println("Running: " + sql);
			res = stmt.executeQuery(sql);
			while (res.next()) {
				System.out.println(res.getString(1) + "\t" + res.getString(2)
						+ "\t" + res.getString(2));
			}

			// Load data into table
			// NOTE: filepath has to be local to the hive server
			// NOTE: /home/user/input.txt is a tab separated file with three
			// fields per line

			String filepath = "/Users/rraizada/Documents/eclipse_workspaces/MRP/MapReduce/src/main/resources/input/hiveinput.txt";
			sql = "load data local inpath '" + filepath + "' into table "
					+ tableName;
			System.out.println("Running: " + sql);

			result = stmt.executeUpdate(sql);
			System.out.println("Returned " + result + ": Table loaded!");

			// Select Query
			sql = "select * from " + tableName + " where id='4'";
			res = stmt.executeQuery(sql);

			// Show tables
			System.out.println("Running: " + sql);
			res = stmt.executeQuery(sql);
			String id = null;
			String name = null;
			String details = null;
			while (res.next()) {
				id = res.getString(1);
				name = res.getString(2);
				details = res.getString(3);
				System.out.println(id + " " + name + " " + details);
			}

			// Registering simple UDF
			registerUDF(stmt, "formatrecord", "hive.udf.FormatRecord");
			sql = "select formatrecord(id, name, details) from " + tableName
					+ " where id='4'";
			// Testing using hard-coded value
			// sql = "select formatrecord(\"Roopali\")";// from orders where id
			// = 4";
			res = stmt.executeQuery(sql);
			while (res.next()) {
				System.out.println(res.getString(1));
			}
			
			//Registering generic UDF
			/*registerUDF(stmt, "containsString", "hive.udf.ComplexUDFExample");

			// Testing using hard-coded value
			List<String> listStr = new ArrayList<String>();
			listStr.add("a");
			listStr.add("b");
			listStr.add("c");
			ComplexUDFExample ex = new ComplexUDFExample();
			// How to call?????
			//sql = "containsString(array(listStr, 'b'))";
			//res = stmt.executeQuery(sql);
			while (res.next()) {
				System.out.println(res.getString(1));
			}*/
		} finally {
			if (res != null)
				res.close();
			stmt.close();
			con.close();
		}
	}
}
