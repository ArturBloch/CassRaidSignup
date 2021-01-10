package cassdemo.tests;

import cassdemo.StressTest;
import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;
import cassdemo.tables.Group;
import cassdemo.tables.User;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestData {

	public final BackendSession session;
	public static AtomicInteger roleOverflowCounter = new AtomicInteger();
	private Random random;
	private final int NUMBER_OF_THREADS = 6;
	private ExecutorService executorService = Executors.newFixedThreadPool(NUMBER_OF_THREADS);

	public TestData(BackendSession session) {
		this.session = session;
		this.random = new Random();
	}

	public void addData() throws BackendException {
		/*Test users*/
		session.insertUser("Artur");
		session.insertUser("Maciej");
		session.insertUser("Mariusz");
		session.insertUser("Ania");
		session.insertUser("Marian");
		session.insertUser("Dariusz");
		session.insertUser("Konrad");
		session.insertUser("Kamil");
		session.insertUser("Marcin");
		session.insertUser("Karolina");
		session.insertUser("Marianna");


		/*Test groups*/
		session.insertGroup("testowi", 2, 2, 3);
		session.insertGroup("testowi2", 2, 4, 4, 4, 5, 6);
		session.insertGroup("testowi3", 1, 2);
		session.insertGroup("testowi4", 1);

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

		session.insertGroup(name, roleMaxSize);
	}

	/*Adding new user*/
	public void addUser(String name) throws BackendException {
		session.insertUser(name);
	}

	/*Assigning random existing users to random groups*/
	public void addRandomUsersToRandomGroups() throws BackendException {
		List<User> allUsers = session.selectAllUsers();
		List<Group> allGroups = session.selectAllGroups();

		for (int i = 0; i < 1000; i++) {
			User randomUser = allUsers.get(random.nextInt(allUsers.size()));
			Group randomGroup = allGroups.get(random.nextInt(allGroups.size()));
			int randomRole = random.nextInt(randomGroup.getRole_max_spots().size());
			session.insertUserIntoGroup(randomGroup.getGroup_id(), randomUser.getUser_id(), randomRole, "WAITING");
		}
	}

	/*Validate groups*/
	public void validateAllGroups() throws BackendException{
		List<Group> allGroups = session.selectAllGroups();

		for (Group currentGroup : allGroups) {
			session.groupValidation(currentGroup.getGroup_id());
		}

	}

	public void stressTest() {
		System.out.println("RUNNING STRESS TEST, TAKES AROUND 15 SECONDS");
		executorService = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
		for (int i = 0; i < NUMBER_OF_THREADS; i++) {
			executorService.execute(new StressTest(this));
		}
		awaitTerminationAfterShutdown(executorService);
	}

	public void uberStressTest() throws BackendException {
		executorService = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
		for (int i = 0; i < 300; i++) {
			addUser("uberStressTest" + i);
		}
		for (int i = 0; i < 50; i++) {
			addGroup("uberStressGroup" + i);
		}

		System.out.println("RUNNING UBER STRESS TEST, TAKES AROUND 15 SECONDS");

		for (int i = 0; i < NUMBER_OF_THREADS; i++) {
			executorService.execute(new StressTest(this));
		}
		awaitTerminationAfterShutdown(executorService);
	}

	public void awaitTerminationAfterShutdown(ExecutorService threadPool) {
		threadPool.shutdown();
		try {
			if (!threadPool.awaitTermination(15, TimeUnit.SECONDS)) {
				threadPool.shutdownNow();
				System.out.println("SHUTTING DOWN STRESS TEST");
			}
		} catch (InterruptedException ex) {
			threadPool.shutdownNow();
			Thread.currentThread().interrupt();
		}
	}
}
