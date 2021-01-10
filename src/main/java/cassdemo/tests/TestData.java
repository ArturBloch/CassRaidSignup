package cassdemo.tests;

import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;
import cassdemo.tables.Group;
import cassdemo.tables.User;

import java.util.List;
import java.util.Random;

public class TestData {

	private final BackendSession session;
	private Random random;

	public TestData(BackendSession session) {
		this.session = session;
		this.random = new Random();
	}

	public void addData() throws BackendException {
		/*Test users*/
		session.upsertUser("Artur");
		session.upsertUser("Maciej");
		session.upsertUser("Mariusz");
		session.upsertUser("Ania");

		/*Test groups*/
		session.upsertGroup("testowi", 2, 2, 3);
		session.upsertGroup("testowi2", 2, 4, 4, 4, 5, 6);

		List<User> users = session.selectAllUsers();
		List<Group> groups = session.selectAllGroups();
		//System.out.println(session.selectUserById(users.get(0).getUser_id()));

		/*Assigning users into groups*/
		session.insertUserIntoGroup(groups.get(0).getGroup_id(), users.get(0).getUser_id(), 0, "WAITING");
		session.insertUserIntoGroup(groups.get(0).getGroup_id(), users.get(1).getUser_id(), 0, "WAITING");
		session.insertUserIntoGroup(groups.get(0).getGroup_id(), users.get(2).getUser_id(), 0, "WAITING");
		session.insertUserIntoGroup(groups.get(0).getGroup_id(), users.get(3).getUser_id(), 1, "WAITING");

		session.selectAllUsersGroup();

		session.groupValidation(groups.get(0).getGroup_id());
	}

	/*Adding new group with random roles*/
	public void addGroup(String name) throws BackendException {
		int roleLength = random.nextInt(5) + 1;
		Integer[] roleMaxSize = new Integer[roleLength];
		for (int i = 0; i < roleLength; i++) {
			roleMaxSize[i] = random.nextInt(5) + 1;
		}

		session.upsertGroup(name, roleMaxSize);
	}

	/*Adding new user*/
	public void addUser(String name) throws BackendException {
		session.upsertUser(name);
	}

	/*Assigning random existing users to random groups*/
	public void addRandomUsersToRandomGroups() throws BackendException {
		List<User> allUsers = session.selectAllUsers();
		List<Group> allGroups = session.selectAllGroups();

		for (int j = 0; j < allUsers.size(); j++) {
			for (int i = 0; i < allGroups.size() / 2; i++) {
				User randomUser = allUsers.get(random.nextInt(allUsers.size()));
				Group randomGroup = allGroups.get(random.nextInt(allGroups.size()));
				int randomRole = random.nextInt(randomGroup.getRole_max_spots().size());

				session.insertUserIntoGroup(randomGroup.getGroup_id(), randomUser.getUser_id(), randomRole, "WAITING");
			}
		}

	}

	/*Validate groups*/
	public void validateAllGroups() throws BackendException{
		List<Group> allGroups = session.selectAllGroups();

		for (Group currentGroup : allGroups) {
			session.groupValidation(currentGroup.getGroup_id());
		}

	}
}
