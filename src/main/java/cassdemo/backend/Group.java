package cassdemo.backend;

import com.datastax.driver.mapping.annotations.Table;

import java.util.UUID;

@Table(name = "groups")
public class Group {

	UUID group_id;
	String group_name;
	int max_role1;
	int max_role2;
	int max_role3;
	int max_role4;
	int max_role5;

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

	public int getMax_role1() {
		return max_role1;
	}

	public void setMax_role1(int max_role1) {
		this.max_role1 = max_role1;
	}

	public int getMax_role2() {
		return max_role2;
	}

	public void setMax_role2(int max_role2) {
		this.max_role2 = max_role2;
	}

	public int getMax_role3() {
		return max_role3;
	}

	public void setMax_role3(int max_role3) {
		this.max_role3 = max_role3;
	}

	public int getMax_role4() {
		return max_role4;
	}

	public void setMax_role4(int max_role4) {
		this.max_role4 = max_role4;
	}

	public int getMax_role5() {
		return max_role5;
	}

	public void setMax_role5(int max_role5) {
		this.max_role5 = max_role5;
	}

	@Override public String toString() {
		return "Group{" + "group_id=" + group_id + ", group_name='" + group_name + '\'' + ", max_role1=" + max_role1 + ", max_role2=" +
		       max_role2 + ", max_role3=" + max_role3 + ", max_role4=" + max_role4 + ", max_role5=" + max_role5 + '}';
	}
}
