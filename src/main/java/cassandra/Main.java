package cassandra;

import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;
import java.util.UUID;
import cassandra.backend.BackendException;
import cassandra.backend.BackendSession;
import cassandra.tests.TestData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
			keyspace     = properties.getProperty("keyspace");
		} catch (IOException ex) {
			ex.printStackTrace();
		}

		BackendSession session = new BackendSession(contactPoint, keyspace);
		TestData testData = new TestData(session);
		testData.addData();

		Scanner in = new Scanner(System.in);
		do {
			System.out.println(
				"CassRaidSignup\nx - EXIT\nu - Add user\ng - Add group\nadd - Assign users to groups\nval - Validate groups\ndeleteU - " +
				"Delete user from group\nstressTest - light stress test\nuberStressTest - run uber test");
			String input = in.nextLine();
			switch (input) {
				case "x": {
					System.out.println("EXITING");
					session.deleteAll();
					System.exit(0);
				}
				case "u": {
					System.out.println("Type user name: ");
					String name = in.nextLine();
					testData.addUser(name);
					break;
				}
				case "g": {
					System.out.println("Type group name: ");
					String name = in.nextLine();
					testData.addGroup(name);
					break;
				}
				case "add": {
					System.out.println("ASSIGNING RANDOM EXISTING USERS TO RANDOM GROUPS");
					testData.addRandomUsersToRandomGroups();
					session.checkAllGroups();
					break;
				}
				case "val": {
					System.out.println("VALIDATING GROUPS");
					testData.validateAllGroups();
					break;
				}
				case "deleteU": {
					System.out.println("Type userId: ");
					String userId = in.nextLine();
					System.out.println("Type groupId: ");
					String groupId = in.nextLine();
					System.out.println("DELETING USER " + userId + "from " + groupId);
					session.removeUserFromGroup(UUID.fromString(userId), UUID.fromString(groupId));
					break;
				}
				case "stressTest": {
					testData.stressTest();
					testData.session.checkAllGroups();
					System.out.println("ROLES WITH TOO MANY PEOPLE ACCEPTED: " + testData.roleOverflowCounter);
					break;
				}
				case "uberStressTest": {
					testData.uberStressTest();
					testData.session.checkAllGroups();
					System.out.println("ROLES WITH TOO MANY PEOPLE ACCEPTED: " + testData.roleOverflowCounter);
					break;
				}
			}
		} while (true);
	}
}
