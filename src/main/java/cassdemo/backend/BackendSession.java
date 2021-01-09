package cassdemo.backend;

import cassdemo.tables.Group;
import cassdemo.tables.GroupUsers;
import cassdemo.tables.User;
import cassdemo.tables.UsersGroup;
import com.datastax.driver.core.*;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class BackendSession {

	private static final Logger logger = LoggerFactory.getLogger(BackendSession.class);

	public static BackendSession instance = null;
	public static MappingManager manager = null;

	private Session session;

	public BackendSession(String contactPoint, String keyspace) throws BackendException {
		logger.debug("Backend starting");
		Cluster cluster = Cluster.builder().addContactPoint(contactPoint).build();
		cluster.getConfiguration().getCodecRegistry().register(InstantCodec.instance);
		try {
			session = cluster.connect(keyspace);
			manager = new MappingManager(session);
		} catch (Exception e) {
			throw new BackendException("Could not connect to the cluster. " + e.getMessage() + ".", e);
		}
		prepareStatements();
		logger.debug("Backend successfully started");
	}

	private static PreparedStatement SELECT_ALL_FROM_USERS;
	private static PreparedStatement SELECT_ALL_FROM_GROUPS;
	private static PreparedStatement SELECT_ALL_FROM_USERS_GROUP;
	private static PreparedStatement SELECT_ONE_FROM_USERS_GROUP;
	private static PreparedStatement SELECT_ALL_USERS_FROM_GROUP_BY_ID;


	private static PreparedStatement SELECT_ONE_FROM_USERS;
	private static PreparedStatement SELECT_ONE_FROM_GROUPS;

	private static PreparedStatement INSERT_INTO_USERS;
	private static PreparedStatement INSERT_INTO_GROUPS;
	private static PreparedStatement INSERT_USER_INTO_GROUP;

	private static PreparedStatement DELETE_USER_FROM_GROUP_BY_ID;

	private static PreparedStatement DELETE_ALL_FROM_USERS;
	private static PreparedStatement DELETE_ALL_FROM_GROUPS;
	private static PreparedStatement DELETE_ALL_FROM_USERS_GROUP;
	private static PreparedStatement DELETE_ALL_FROM_GROUP_USERS;


	private void prepareStatements() throws BackendException {
		logger.debug("Preparing statements / queries");
		try {
			SELECT_ALL_FROM_USERS             = session.prepare("SELECT * FROM users;");
			SELECT_ALL_FROM_GROUPS            = session.prepare("SELECT * FROM groups;");
			SELECT_ONE_FROM_USERS             = session.prepare("SELECT * FROM users WHERE user_id=?;");
			SELECT_ONE_FROM_GROUPS            = session.prepare("SELECT * FROM groups WHERE group_id=?;");
			SELECT_ALL_USERS_FROM_GROUP_BY_ID = session.prepare("SELECT * FROM group_users WHERE group_id=?;");
			INSERT_INTO_USERS                 = session.prepare("INSERT INTO users (user_id, user_name) VALUES (?, ?);");
			INSERT_INTO_GROUPS                = session.prepare(
				"INSERT INTO groups (group_id, group_name, role_max_spots) VALUES (?, ?, ?);");

			INSERT_USER_INTO_GROUP      = session.prepare(
				"BEGIN BATCH " + "INSERT INTO group_users (group_id, user_id, roleName, addedAt, status) VALUES (?, ?, ?, ?, ?);" +
				"INSERT INTO users_group (user_id, group_id, roleName, addedAt, status) VALUES (?, ?, ?, ?, ?);" + "APPLY BATCH;");
			SELECT_ALL_FROM_USERS_GROUP = session.prepare("SELECT * FROM users_group;");
			SELECT_ONE_FROM_USERS_GROUP = session.prepare("SELECT * FROM users_group WHERE user_id=? AND group_id=?;");

			DELETE_USER_FROM_GROUP_BY_ID = session.prepare(
				"BEGIN BATCH DELETE FROM group_users WHERE group_id=? AND user_id=?; DELETE FROM users_group WHERE " +
				"user_id=? AND group_id=?; APPLY BATCH;");
			DELETE_ALL_FROM_USERS        = session.prepare("TRUNCATE users;");
			DELETE_ALL_FROM_GROUPS       = session.prepare("TRUNCATE groups;");
			DELETE_ALL_FROM_USERS_GROUP  = session.prepare("TRUNCATE group_users;");
			DELETE_ALL_FROM_GROUP_USERS  = session.prepare("TRUNCATE users_group;");
		} catch (Exception e) {
			throw new BackendException("Could not prepare statements. " + e.getMessage() + ".", e);
		}

		logger.debug("Statements /queries prepared");
	}

	public List<User> selectAllUsers() throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_ALL_FROM_USERS);
		Mapper<User> userMapper = manager.mapper(User.class);

		ResultSet rs = null;
		List<User> selectedUsers = new ArrayList<>();

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		selectedUsers = userMapper.map(rs).all();

		for (User selectedUser : selectedUsers) {
			logger.info(String.valueOf(selectedUser));
		}

		return selectedUsers;
	}

	public User selectUserById(UUID userId) throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_ONE_FROM_USERS);
		bs.bind(userId);
		Mapper<User> userMapper = manager.mapper(User.class);

		ResultSet rs;
		User selectedUser;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		selectedUser = userMapper.map(rs).one();
		logger.debug("User found by ID " + selectedUser);
		return selectedUser;
	}

	public Group selectedGroupById(UUID groupId) throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_ONE_FROM_GROUPS);
		bs.bind(groupId);
		Mapper<Group> groupMapper = manager.mapper(Group.class);

		ResultSet rs;
		Group selectedGroup;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		selectedGroup = groupMapper.map(rs).one();
		logger.debug("Group found by ID " + selectedGroup);
		return selectedGroup;
	}

	public List<UsersGroup> selectAllUsersGroup() throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_ALL_FROM_USERS_GROUP);
		Mapper<UsersGroup> usersGroupMapper = manager.mapper(UsersGroup.class);

		ResultSet rs = null;
		List<UsersGroup> selectResults = new ArrayList<>();

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		selectResults = usersGroupMapper.map(rs).all();

		for (UsersGroup result : selectResults) {
			logger.info(String.valueOf(result));
		}

		return selectResults;
	}

	public UsersGroup selectOneUserGroup(UUID userId, UUID groupId) throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_ONE_FROM_USERS_GROUP);
		bs.bind(userId, groupId);
		Mapper<UsersGroup> usersGroupMapper = manager.mapper(UsersGroup.class);

		ResultSet rs = null;
		UsersGroup selectedResult;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		selectedResult = usersGroupMapper.map(rs).one();
		logger.info(String.valueOf(selectedResult));

		return selectedResult;
	}


	public void removeUserFromGroup(UUID userId, UUID groupId) throws BackendException {
		logger.debug("REMOVING " + userId + " from " + groupId);
		List<GroupUsers> groupUsers = selectAllGroupUsersByGroupId(groupId);

		BoundStatement bs = new BoundStatement(DELETE_USER_FROM_GROUP_BY_ID);
		bs.bind(groupId, userId, userId, groupId);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		groupUsers.removeIf(e -> e.getUser_id().equals(userId) && e.getGroup_id().equals(groupId));

		groupValidation(groupId, groupUsers);
	}

	public void groupValidation(UUID groupId) throws BackendException {
		groupValidation(groupId, null);
	}

	public void groupValidation(UUID groupId, List<GroupUsers> groupUsersList) throws BackendException {
		Group chosenGroup = selectedGroupById(groupId);
		List<GroupUsers> groupUsers;

		if(groupUsersList == null){
			groupUsers = selectAllGroupUsersByGroupId(groupId);
		} else {
			groupUsers = groupUsersList;
		}

		groupUsers.sort(Comparator.comparing(GroupUsers::getAddedAt));

		for (int i = 0; i < chosenGroup.getRole_max_spots().size(); i++) {
			int roleId = i;

			int countUsersAccepted = (int) groupUsers.stream()
			                                         .filter(e -> e.getRoleName() == roleId && e.getStatus().equals("ACCEPTED"))
			                                         .count();

			int howManyFreeSpots = chosenGroup.getRole_max_spots().get(roleId) - countUsersAccepted;
			if (howManyFreeSpots > 0) {
				System.out.println("ACCEPTING USERS");
				System.out.println("FREE SPOTS " + howManyFreeSpots);
				List<GroupUsers> availableGroupUsers = groupUsers.stream()
				                                                 .filter(
					                                                 e -> !e.getStatus().equals("ACCEPTED") && e.getRoleName() == roleId)
				                                                 .collect(Collectors.toList());

				for (int j = 0; j < availableGroupUsers.size(); j++) {
					if (howManyFreeSpots == 0) break;
					howManyFreeSpots--;
					GroupUsers currentGroupUser = availableGroupUsers.get(j);
					currentGroupUser.setStatus("ACCEPTED");
					insertUserIntoGroup(groupId, currentGroupUser.getUser_id(), roleId, "ACCEPTED", currentGroupUser.getAddedAt());
				}
			}

			List<GroupUsers> rejectedGroupUsers = groupUsers.stream()
			                                                .filter(e -> !e.getStatus().equals("ACCEPTED") && e.getRoleName() == roleId)
			                                                .collect(Collectors.toList());
			System.out.println("REJECTING USERS");
			for (GroupUsers rejectedGroupUser : rejectedGroupUsers) {
				insertUserIntoGroup(groupId, rejectedGroupUser.getUser_id(), roleId, "REJECTED", rejectedGroupUser.getAddedAt());
			}

		}
	}

	public List<GroupUsers> selectAllGroupUsersByGroupId(UUID groupId) throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_ALL_USERS_FROM_GROUP_BY_ID);
		bs.bind(groupId);
		Mapper<GroupUsers> groupUsersMapper = manager.mapper(GroupUsers.class);

		ResultSet rs = null;
		List<GroupUsers> selectResults = new ArrayList<>();

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		selectResults = groupUsersMapper.map(rs).all();

		for (GroupUsers result : selectResults) {
			logger.info(String.valueOf(result));
		}

		return selectResults;
	}

	public List<Group> selectAllGroups() throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_ALL_FROM_GROUPS);
		Mapper<Group> groupMapper = manager.mapper(Group.class);

		ResultSet rs = null;
		List<Group> selectedGroups = new ArrayList<>();

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		selectedGroups = groupMapper.map(rs).all();

		for (Group selectedGroup : selectedGroups) {
			logger.info(String.valueOf(selectedGroup));
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

	public void upsertGroup(String groupName, Integer... maxUsers) throws BackendException {

		UUID newUUID = UUID.randomUUID();
		BoundStatement bs = new BoundStatement(INSERT_INTO_GROUPS);
		bs.bind(newUUID, groupName, Arrays.asList(maxUsers));

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
		}

		logger.info("Group " + groupName + " upserted with id: " + newUUID);
	}

	public void insertUserIntoGroup(UUID groupId, UUID userId, int roleNumber, String status, Instant timestamp) throws BackendException {
		BoundStatement bs = new BoundStatement(INSERT_USER_INTO_GROUP);

		UsersGroup prevUsersGroup = selectOneUserGroup(userId, groupId);

		if(prevUsersGroup != null){
			if(status.equals(prevUsersGroup.getStatus())){
				if (prevUsersGroup.getRoleName() == roleNumber) {
					return;
				} else {
					timestamp = prevUsersGroup.getAddedAt();
					status    = "WAITING";
				}
			}
		}

		bs.bind(groupId, userId, roleNumber, timestamp, status, userId, groupId, roleNumber, timestamp, status);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform insert operation. " + e.getMessage() + ".", e);
		}
		//TODO: change logger info
		logger.info("Inserted user into group");
	}

	public void insertUserIntoGroup(UUID groupId, UUID userId, int roleNumber, String status) throws BackendException {
		insertUserIntoGroup(groupId, userId, roleNumber, status, Instant.now());
	}

	public void deleteAll() throws BackendException {
		BoundStatement bs = new BoundStatement(DELETE_ALL_FROM_USERS);
		BoundStatement bs1 = new BoundStatement(DELETE_ALL_FROM_GROUPS);
		BoundStatement bs2 = new BoundStatement(DELETE_ALL_FROM_USERS_GROUP);
		BoundStatement bs3 = new BoundStatement(DELETE_ALL_FROM_GROUP_USERS);

		try {
			session.execute(bs);
			session.execute(bs1);
			session.execute(bs2);
			session.execute(bs3);
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
