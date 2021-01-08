package cassdemo;

import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;

import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;

public class Main {

	private static final String PROPERTIES_FILENAME = "config.properties";

	public static void main(String[] args) throws IOException, BackendException {
		System.out.println("Started main");
		String contactPoint = null;
		String keyspace = null;

		Properties properties = new Properties();
		try {
			properties.load(Main.class.getClassLoader().getResourceAsStream(PROPERTIES_FILENAME));

			contactPoint = properties.getProperty("contact_point");
			keyspace = properties.getProperty("keyspace");
		} catch (IOException ex) {
			ex.printStackTrace();
		}

		System.out.println("before backend");

		BackendSession session = new BackendSession(contactPoint, keyspace);

		System.out.println("after backend");


		session.upsertUser("Artur");
		session.upsertUser("Maciej");
		session.upsertUser("Mariusz");

		session.upsertGroup("testowi", 1, 2, 3, 4, 5);
		session.upsertGroup("testowi2", 4, 4, 4, 4, 4);

		session.selectAllUsers();


//		String output = session.selectAll();
//		session.deleteAll();

		System.out.println("Reading input");

		Scanner in = new Scanner(System.in);
		do{
			System.out.println("Type x, u, g, testA, testB");
			String input = in.nextLine();
			switch(input){
				case "x" : {
					System.out.println("EXITING");
					System.exit(0);
				}
				case "u" : {
					System.out.println("Adding users");
					break;
				}
				case "g" : {
					System.out.println("Adding groups");
					break;
				}
				case "testA" : {
					System.out.println("Running testA");
					break;
				}
				case "testB" : {
					System.out.println("Running testB");
					break;
				}
			}
		} while(true);
	}
}
