package cassandra.tables;

import com.datastax.driver.mapping.annotations.Table;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Table(name = "groups")
public class Group {

	UUID group_id;
	String group_name;
	List<Integer> role_max_spots;

	public UUID getGroup_id() {
		return group_id;
	}

	public void setGroup_id(UUID group_id) {
		this.group_id = group_id;
	}

	public String getGroup_name() {
		return group_name;
	}

	public void setGroup_name(String group_name) {
		this.group_name = group_name;
	}

	public List<Integer> getRole_max_spots() {
		return role_max_spots;
	}

	public void setRole_max_spots(List<Integer> role_max_spots) {
		this.role_max_spots = role_max_spots;
	}

	@Override public String toString() {
		return "Group{" + "group_id=" + group_id + ", group_name='" + group_name + '\'' + ", role_max_spots=" + role_max_spots.stream().map(Object::toString)
		                                                                                                            .collect(
			                                                                                                            Collectors.joining(", ")) + '}';
	}
}
