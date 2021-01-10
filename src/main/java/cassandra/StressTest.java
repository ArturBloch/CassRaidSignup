package cassandra;

import cassandra.backend.BackendException;
import cassandra.tests.TestData;

public class StressTest implements Runnable{

	private final TestData testData;

	public StressTest(TestData testData) {
		this.testData = testData;
	}

	@Override public void run() {
		try {
			testData.addRandomUsersToRandomGroups();
		} catch (BackendException e) {
			e.printStackTrace();
		}
	}
}
