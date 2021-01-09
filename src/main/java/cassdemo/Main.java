package cassdemo;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.UUID;

import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;
import cassdemo.tables.Group;
import cassdemo.tables.User;
import cassdemo.tests.TestData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: usuwanie grup
// TODO: usuwanie użytkowników z grup (teoretycznie ponowna walidacja roli usuniętego użytkownika jeśli był on accepted)
// TODO: zmiana ról użytkownika


public class Main {

	private static final String PROPERTIES_FILENAME = "config.properties";
	private static final Logger logger = LoggerFactory.getLogger(Main.class);

	public static void main(String[] args) throws IOException, BackendException {
		logger.debug("Main started");
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

		TestData testData = new TestData(session);
		testData.addData();

		System.out.println("Reading input");

		Scanner in = new Scanner(System.in);
		do{
			System.out.println("Type x, u, g, add, val, deleteU");
			String input = in.nextLine();
			switch(input){
				case "x" : {
					System.out.println("EXITING");
					session.deleteAll();
					System.exit(0);
				}
				case "u" : {
					System.out.println("Type user name: ");
					String name = in.nextLine();
					testData.addUser(name);
					break;
				}
				case "g" : {
					System.out.println("Type group name: ");
					String name = in.nextLine();
					testData.addGroup(name);
					break;
				}
				case "add" : {
					System.out.println("ADDING RANDOM USERS TO RANDOM GROUPS");
					testData.addRandomUsersToRandomGroups();
					break;
				}
				case "val" : {
					System.out.println("VALIDATING GROUPS");
					testData.validateAllGroups();
					break;
				}
				case "deleteU": {
					System.out.println("DELETING USER, GIVE USER ID");
					String userID = in.nextLine();
					System.out.println("GIVE GROUP ID");
					String groupID = in.nextLine();
					session.removeUserFromGroup(UUID.fromString(userID), UUID.fromString(groupID));
					break;
				}
			}
		} while(true);
	}
}
