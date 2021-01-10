package cassdemo;

import cassdemo.backend.BackendException;
import cassdemo.tests.TestData;

public class StressTest implements Runnable{

	private TestData testData;

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