# CassRaidSignup

### PROJECT IDEA

This application allows for signing up users to groups.  
Each group consists of multiple roles(given during group creation) and every role has limited spots for users to sign up.  
The rule used for sign ups is "first come, first served" which means that users are added to groups with  a timestamp of their sign up and based on that and 
availability of spots in the groups they are accepted during final group validation.

### DATABASE SCHEMA

TABLES:

- groups (group_id uuid, group_name text, role_max_spots list<int>)
- users (user_id uuid, user_name text)
- group_users (group_id uuid, user_id uuid, roleName int, addedAt timestamp, status text)
- users_group (user_id uuid, group_id uuid, roleName int, addedAt timestamp, status text)

### HOW TO USE THIS?

1. Create your own local Cassandra cluster
2. Build the project with Gradle Build
3. Run the project with Gradle Run
4. Use the interface as specified in the application, examples:  
   a) Adding users  
   b) Adding groups  
   c) Running stress Tests

### AUTHORS

Maciej Łaszkiewicz  
Artur Bloch




