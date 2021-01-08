package cassdemo.backend;

import com.datastax.driver.core.*;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import org.joda.time.convert.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;



/*
 * For error handling done right see:
 * https://www.datastax.com/dev/blog/cassandra-error-handling-done-right
 *
 * Performing stress tests often results in numerous WriteTimeoutExceptions,
 * ReadTimeoutExceptions (thrown by Cassandra replicas) and
 * OpetationTimedOutExceptions (thrown by the client). Remember to retry
 * failed operations until success (it can be done through the RetryPolicy mechanism:
 * https://stackoverflow.com/questions/30329956/cassandra-datastax-driver-retry-policy )
 */

public class BackendSession {

	private static final Logger logger = LoggerFactory.getLogger(BackendSession.class);

	public static BackendSession instance = null;
	public static MappingManager manager = null;

	private Session session;

	public BackendSession(String contactPoint, String keyspace) throws BackendException {
		Cluster cluster = Cluster.builder().addContactPoint(contactPoint).build();
		try {
			session = cluster.connect(keyspace);
			manager = new MappingManager(session);
		} catch (Exception e) {
			throw new BackendException("Could not connect to the cluster. " + e.getMessage() + ".", e);
		}
		prepareStatements();
	}

	private static PreparedStatement SELECT_ALL_FROM_USERS;
	private static PreparedStatement SELECT_ALL_FROM_GROUPS;
	private static PreparedStatement SELECT_ALL_FROM_USERS_GROUP;
	private static PreparedStatement SELECT_ALL_FROM_GROUP_USERS;

	private static PreparedStatement SELECT_ONE_FROM_USERS;
	private static PreparedStatement SELECT_ONE_FROM_GROUPS;

	private static PreparedStatement INSERT_INTO_USERS;
	private static PreparedStatement INSERT_INTO_GROUPS;
	private static PreparedStatement INSERT_USER_INTO_GROUP;

	private static PreparedStatement DELETE_ALL_FROM_USERS;
	private static PreparedStatement DELETE_ALL_FROM_GROUPS;

	private void prepareStatements() throws BackendException {
		try {
			SELECT_ALL_FROM_USERS = session.prepare("SELECT * FROM users;");
			SELECT_ALL_FROM_GROUPS = session.prepare("SELECT * FROM groups;");

			INSERT_INTO_USERS     = session.prepare("INSERT INTO users (user_id, user_name) VALUES (?, ?);");
			INSERT_INTO_GROUPS    = session.prepare(
				"INSERT INTO groups (group_id, group_name, max_role1, max_role2, max_role3, max_role4, max_role5) VALUES (?, ?, ?, ?, ?," +
				" " + "?, ?" + ")" + ";");

			INSERT_USER_INTO_GROUP = session.prepare(
				"BEGIN BATCH " +
				"INSERT INTO group_users (group_id, user_id, roleName, addedAt, status) VALUES (?, ?, ?, ?, ?);" +
				"INSERT INTO users_group (user_id, group_id, roleName, addedAt, status) VALUES (?, ?, ?, ?, ?);" +
				"APPLY BATCH;" );
			SELECT_ALL_FROM_USERS_GROUP = session.prepare("SELECT * FROM users_group;");
			SELECT_ALL_FROM_GROUP_USERS = session.prepare("SELECT * FROM group_users;");
			DELETE_ALL_FROM_USERS = session.prepare("TRUNCATE users;");
		} catch (Exception e) {
			throw new BackendException("Could not prepare statements. " + e.getMessage() + ".", e);
		}

		logger.info("Statements prepared");
	}

	public List<User> selectAllUsers() throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_ALL_FROM_USERS);
		Mapper<User> userMapper =  manager.mapper(User.class);

		ResultSet rs = null;
		List<User> selectedUsers = new ArrayList<>();

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		selectedUsers = userMapper.map(rs).all();

		for (User selectedUser : selectedUsers) {
			System.out.println(selectedUser);
		}

		return selectedUsers;
	}

	public List<UsersGroup> selectAllUsersGroup() throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_ALL_FROM_USERS_GROUP);
		Mapper<UsersGroup> usersGroupMapper =  manager.mapper(UsersGroup.class);

		ResultSet rs = null;
		List<UsersGroup> selectResults = new ArrayList<>();

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		selectResults = usersGroupMapper.map(rs).all();

		for (UsersGroup result : selectResults) {
			System.out.println(result);
		}

		return selectResults;
	}

	public List<Group> selectAllGroups() throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_ALL_FROM_GROUPS);
		Mapper<Group> groupMapper =  manager.mapper(Group.class);

		ResultSet rs = null;
		List<Group> selectedGroups = new ArrayList<>();

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		selectedGroups = groupMapper.map(rs).all();

		for (Group selectedGroup : selectedGroups) {
			System.out.println(selectedGroup);
		}

		return selectedGroups;
	}


	public void upsertUser(String userName) throws BackendException {
		UUID newUUID = UUID.randomUUID();

		BoundStatement bs = new BoundStatement(INSERT_INTO_USERS);
		bs.bind(newUUID, userName);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
		}

		logger.info("User " + userName + " upserted with id: " + newUUID);
	}

	public void upsertGroup(String groupName, int... maxUsers) throws BackendException {
		if(maxUsers.length < 5) return;

		UUID newUUID = UUID.randomUUID();
		BoundStatement bs = new BoundStatement(INSERT_INTO_GROUPS);
		bs.bind(newUUID, groupName, maxUsers[0], maxUsers[1], maxUsers[2], maxUsers[3], maxUsers[4]);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
		}

		logger.info("User " + groupName + " upserted with id: " + newUUID);
	}

	public void insertUserIntoGroup(UUID groupId, UUID userId, int roleNumber, String status) throws BackendException {
		BoundStatement bs = new BoundStatement(INSERT_USER_INTO_GROUP);
		ZonedDateTime dateTime = ZonedDateTime.from(ZonedDateTime.now());
		Timestamp timestamp = Timestamp.valueOf(dateTime.toLocalDateTime());

		bs.bind(groupId, userId, roleNumber, timestamp, status, userId, groupId, roleNumber, timestamp, status);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a delete operation. " + e.getMessage() + ".", e);
		}

		System.out.println("User added to group");
		logger.info("All users deleted");
	}



	public void deleteAll() throws BackendException {
		BoundStatement bs = new BoundStatement(DELETE_ALL_FROM_USERS);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a delete operation. " + e.getMessage() + ".", e);
		}

		logger.info("All users deleted");
	}

	protected void finalize() {
		try {
			if (session != null) {
				session.getCluster().close();
			}
		} catch (Exception e) {
			logger.error("Could not close existing cluster", e);
		}
	}
}
