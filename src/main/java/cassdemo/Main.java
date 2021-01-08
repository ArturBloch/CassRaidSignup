package cassdemo;

import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;

import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;

public class Main {

	private static final String PROPERTIES_FILENAME = "config.properties";

	public static void main(String[] args) throws IOException, BackendException {
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
			
		BackendSession session = new BackendSession(contactPoint, keyspace);

		session.upsertUser("Artur");
		session.upsertUser("Maciej");
		session.upsertUser("Mariusz");

		session.upsertGroup("testowi", 1, 2, 3, 4, 5);
		session.upsertGroup("testowi2", 4, 4, 4, 4, 4);


		String output = session.selectAll();
		System.out.println("Users: \n" + output);

		session.deleteAll();

		Scanner in = new Scanner(System.in);

		do{
			String input = in.next();
			System.out.println("Type x, u, g, testA, testB");
			switch(input){
				case "x" : {
					return;
				}
				case "u" : {
					break;
				}
				case "g" : {
					break;
				}
				case "testA" : {
					break;
				}
				case "testB" : {
					break;
				}
			}
		} while(true);
	}
}
