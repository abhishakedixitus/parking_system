import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import java.util.*;

public class LoadCassandraData {

	// Reading the File
	private static String Key;
	private static String Location;
	private static String Zip;
	private static String In_Time;
	private static String Out_Time;
	private static String Side;
	private static String Unit_Desc;
	private static String Parking_Category;
	private static String Total_Parking_Slots;
	private static String Total_slots_filled;
	private static String Sub_area;
	private static String Status;
	private static Scanner userInput2;

	public static void main(String[] args) {

		// Creating Cluster object
		Cluster cluster = Cluster.builder().addContactPoints("127.0.0.1").build();

		// Creating Session object
		Session session = cluster.connect();

		// Dropping Keyspace
		String query_keyspace1 = "DROP KEYSPACE ParkingSystem;";
		session.execute(query_keyspace1);
		System.out.println("KeySpace Dropped Successfully");

		// Creating Keyspace
		String query_keyspace = "CREATE KEYSPACE ParkingSystem WITH replication "
				+ "={'class' : 'SimpleStrategy', 'replication_factor':2};";

		// Creating Table
		// String query = "CREATE TABLE Parking (licenseNum text PRIMARY KEY, " +
		// "Parking_Lot_Number text, "
		// + "Location text);";

		// String query = "CREATE TABLE Parking (licenseNum text PRIMARY KEY,
		// Parking_Lot_Number text, Location text);";
		String query = "CREATE TABLE Parking (Key text PRIMARY KEY, Location text, Zip text, In_Time text, Out_Time text, Side text, Unit_Desc text, "
				+ "Parking_Category text, Total_Parking_Slots text, Total_slots_filled text, Sub_area text, Status text);";

		// Creating Cluster object
		// Cluster cluster = Cluster.builder().addContactPoints("127.0.0.1").build();

		// Creating Session object
		// Session session = cluster.connect();

		System.out.println("Connection Open");

		session.execute(query_keyspace);
		System.out.println("KeySpace Created Successfully");

		session.execute("use ParkingSystem");
		session.execute(query);

		System.out.println("Table Created Successfully");
		// Reading Data
		int linecount = 0;
		try {
			FileReader fr = new FileReader("src/data/Parking.csv");
			BufferedReader bf = new BufferedReader(fr);
			String line;
		//	System.out.println(bf.readLine());
			while ((line = bf.readLine()) != null) {
			
			linecount++;
				String[] a = line.split(",");
				System.out.println("Line Count Now is ->" + linecount);
				Key = a[0];
				Location = a[1];
				Zip = a[2];
				In_Time = a[2];
				Out_Time = a[3];
				Side = a[4];
				Unit_Desc = a[5];
				Parking_Category = a[6];
				Total_Parking_Slots = a[7];
				Total_slots_filled = a[8];
				Sub_area = a[9];
				Status = a[10];

				// Writing the insert queries
				PreparedStatement statement = session.prepare(
						"INSERT INTO ParkingSystem.parking (Key, Location, Zip, In_Time, Out_Time, Side, Unit_Desc, Parking_Category, Total_Parking_Slots, Total_slots_filled, Sub_area, Status) "
								+ " VALUES(?, ?, ?, ? ,? ,? ,? ,? ,? ,? ,? ,? )");

				// create the Bound Statement and initialize it with prepared statement
				BoundStatement bs = new BoundStatement(statement);

				session.execute(bs.bind(Key, Location, Zip, In_Time, Out_Time, Side, Unit_Desc, Parking_Category,
						Total_Parking_Slots, Total_slots_filled, Sub_area, Status));

				System.out.println("Records Added Successfully");

				// Retrieving Data
				System.out.println("Retrieving Data");
				String query_read = "Select * from parking";
				ResultSet result = session.execute(query_read);
				System.out.println(result.all() + "\n");

				// Menu-Driven
			    Scanner userInput = new Scanner(System.in);
			    int choice;
				
				do {
					System.out.println("Welcome to Save My Spot : ABC City");
					System.out.println("Now finding your parking becomes easier");
					System.out.println("Please choose from the following options");
					System.out.println("1. Where do you want your spot");
					System.out.println("2. See all Spots");
					System.out.println("3. Exit");

					// get User Input

	               // int choice = 0;
				// Scanner userInput2 = new Scanner(System.in);
					
					choice = userInput.nextInt();
                    
				    // switch case for all possible choices
					switch (choice) {
					case 1:
						System.out.println("You have following locations");
						System.out.println(
								"Uptown\r\n 12th Ave - Weekday\r\n12th Avenue\r\n15th Avenue\r\nBallard\r\nBallard - Weekday\r\nBallard Locks\r\nBallard Locks - Weekday (Spring)\r\nBallard Locks - Weekday (Summer)\r\nBallard Locks Spring");
						// get user input
						Scanner userInput1 = new Scanner(System.in);
						String choice1 = userInput1.nextLine();
						String query_filter = ("Select * from ParkingSystem.parking where location = ? ALLOW FILTERING");
						System.out.println("Below are the Parking Locations available for the selected Location");
						ResultSet result1 = session.execute(query_filter, choice1);
						System.out.println(result1.all() + "\n");
					//	break;

					case 2:
						System.out.println("Here are all the spots");
						String query_all = ("Select * from ParkingSystem.parking ALLOW FILTERING");
						System.out.println("Below are all the spots across locations");
						ResultSet result2 = session.execute(query_all);
						System.out.println(result2.all() + "\n");
					//	break;

					case 3:
						System.out.println("Thanks for Visiting. Visit Again!");
					//	break;

					default:
						System.out.println("You have entered invalid choice");
					}    
				} while (choice <= 3); 

			} // end of while
		
			bf.close();

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			System.out.println("Just here");
		}

		/*
		 * System.out.println("Records Added Successfully");
		 * 
		 * // Retrieving Data System.out.println("Retrieving Data"); String query_read =
		 * "Select * from parking"; ResultSet result = session.execute(query_read);
		 * System.out.println(result.all() + "\n");
		 */
		// declare a scanner for user Input

		/*
		 * System.out.println("Select the Location"); System.out.println("AndersonSt");
		 * System.out.println("HollywoodDr"); System.out.println("WellBorbRd");
		 * 
		 * // get user input Scanner userInput1 = new Scanner(System.in); String choice
		 * = userInput.nextLine(); String query_filter =
		 * ("Select * from ParkingSystem.parking where location = ? ALLOW FILTERING");
		 * ResultSet result1 = session.execute(query_filter, choice);
		 * System.out.println(result1.all() + "\n");
		 */

		session.close();
		cluster.close();
		System.out.println("Connection Closed");
	}

}
