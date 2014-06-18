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
