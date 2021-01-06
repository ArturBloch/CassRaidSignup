CREATE KEYSPACE IF NOT EXISTS Test
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE Test;

CREATE TABLE IF NOT EXISTS groups
(
    group_id uuid,
    group_name text,
    max_role1 int,
    max_role2 int,
    max_role3 int,
    max_role4 int,
    max_role5 int,
    PRIMARY KEY (group_id)
);

CREATE TABLE IF NOT EXISTS users
(
    user_id uuid,
    user_name,
    PRIMARY KEY (user_id)
);

CREATE TABLE IF NOT EXISTS group_users
(
    group_id uuid,
    user_id uuid,
    roleName text,
    addedAt timestamp,
    PRIMARY KEY (group_id, user_id)
);

CREATE TABLE IF NOT EXISTS users_group
(
    user_id uuid,
    group_id uuid,
    roleName text,
    addedAt timestamp,
    PRIMARY KEY (user_id, group_id)
);
