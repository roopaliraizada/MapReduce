package hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveJdbcClientExample {
	/*
	 * 
	 * Before Running this example we should start thrift server. To Start
	 * Thrift server we should run below command in terminal hive --service
	 * hiveserver &
	 */
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}

		Connection con = DriverManager.getConnection(
				"jdbc:hive2://localhost:10000/default", "rraizada",
				"impetus123");
		Statement stmt = con.createStatement();
		ResultSet res = null;
		String tableName = "orders";
		int result = stmt.executeUpdate("drop table " + tableName);
		System.out.println("Returned " + result + ": Table dropped!");
		result = stmt
				.executeUpdate("create table "
						+ tableName
						+ " (id int, name string, details string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'");
		System.out.println("Returned " + result + ": Table created!");
		// show tables
		String sql = "show tables"; // '" + tableName + "'";
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		if (res.next()) {
			System.out.println(res.getString(1));
		}

		// show create tables
		/*sql = "show create table " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);

		if (res.next()) {
			System.out.println(res.getString(1));
		}*/

		// describe table
		sql = "describe " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1) + "\t" + res.getString(2)
					+ "\t" + res.getString(2));
		}

		// load data into table
		// NOTE: filepath has to be local to the hive server
		// NOTE: /home/user/input.txt is a ctrl-A separated file with three fields per line

		String filepath = "/Users/rraizada/Documents/eclipse_workspaces/MRP/MapReduce/src/main/resources/input/hiveinput.txt";
		sql = "load data local inpath '" + filepath + "' into table " + tableName;
		System.out.println("Running: " + sql);

		result = stmt.executeUpdate(sql);
		System.out.println("Returned " + result + ": Table loaded!");
		sql = "select * from " + tableName + " where id='4'";
		res = stmt.executeQuery(sql);
		// show tables
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
		
		/*sql = "select formatrecord(id, name, details) from " + tableName + " where id='4'";
		res = stmt.executeQuery(sql);
		while(res.next()){
			System.out.println(res.getString(1));
		}*/
		if (res != null)
			res.close();
		stmt.close();
		con.close();
	}
}
