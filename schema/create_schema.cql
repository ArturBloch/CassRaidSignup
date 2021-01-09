CREATE KEYSPACE IF NOT EXISTS Test
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE Test;

CREATE TABLE IF NOT EXISTS groups
(
    group_id uuid,
    group_name text,
    role_max_spots list<int>,
    PRIMARY KEY (group_id)
);

CREATE TABLE IF NOT EXISTS users
(
    user_id uuid,
    user_name text,
    PRIMARY KEY (user_id)
);

CREATE TABLE IF NOT EXISTS group_users
(
    group_id uuid,
    user_id uuid,
    roleName int,
    addedAt timestamp,
    status text,
    PRIMARY KEY (group_id, user_id)
);

CREATE TABLE IF NOT EXISTS users_group
(
    user_id uuid,
    group_id uuid,
    roleName int,
    addedAt timestamp,
    status text,
    PRIMARY KEY (user_id, group_id)
);
