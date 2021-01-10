package cassandra.tables;

import com.datastax.driver.mapping.annotations.Table;

import java.util.UUID;

@Table(name = "users")
public class User {

	UUID user_id;
	String user_name;


	public UUID getUser_id() {
		return user_id;
	}

	public void setUser_id(UUID user_id) {
		this.user_id = user_id;
	}

	public String getUser_name() {
		return user_name;
	}

	public void setUser_name(String user_name) {
		this.user_name = user_name;
	}

	@Override public String toString() {
		return "User{" + "user_id=" + user_id + ", user_name='" + user_name + '\'' + '}';
	}
}
