package cassandra.tables;

import com.datastax.driver.mapping.annotations.Table;

import java.time.Instant;
import java.util.UUID;

@Table(name = "group_users")
public class GroupUsers {

	UUID group_id;
	UUID user_id;
	int roleName;
	Instant addedAt;
	String status;

	public UUID getGroup_id() {
		return group_id;
	}

	public void setGroup_id(UUID group_id) {
		this.group_id = group_id;
	}

	public UUID getUser_id() {
		return user_id;
	}

	public void setUser_id(UUID user_id) {
		this.user_id = user_id;
	}

	public int getRoleName() {
		return roleName;
	}

	public void setRoleName(int roleName) {
		this.roleName = roleName;
	}

	public Instant getAddedAt() {
		return addedAt;
	}

	public void setAddedAt(Instant addedAt) {
		this.addedAt = addedAt;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	@Override public String toString() {
		return "GroupUsers{" + "group_id=" + group_id + ", user_id=" + user_id + ", roleName=" + roleName + ", addedAt=" + addedAt +
		       ", status='" + status + '\'' + '}';
	}
}
