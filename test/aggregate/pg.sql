-- Db-spec for PostgreSQL

{:classname "org.postgresql.Driver"
 :subprotocol "postgresql"
 :subname "testdb"
 :user "riemensc"
 :password "test"}



-- Project

create table customer (id serial primary key, name varchar(30));
create table person (id serial primary key, name varchar(30));
create table project (id serial primary key, name varchar(30), manager_id integer references person(id), customer_id integer references customer(id));
create table task (id serial primary key, description varchar(50), effort integer, project_id integer, assignee_id integer references person(id));
create table person_project (project_id integer references project(id), person_id integer references person(id));


drop table task;
drop table person_project;
drop table project;
drop table customer;
drop table person;


-- Scenario 1

create table a (id serial primary key, name varchar(30));
create table c (id serial primary key, name varchar(30));
create table b (id serial primary key, name varchar(30), a_id integer references a(id), c_id integer references c(id));


drop table b;
drop table c;
drop table a;


-- Scenario 2

create table b (id serial primary key, name varchar(30));
create table a (id serial primary key, name varchar(30), b_id integer references b(id));
create table c (id serial primary key, name varchar(30));
create table b_c (b_id integer references b(id), c_id integer references c(id));


drop table b_c;
drop table a;
drop table b;
drop table c;
