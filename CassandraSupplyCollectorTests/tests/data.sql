CREATE KEYSPACE IF NOT EXISTS test WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' };
USE test;

DROP TABLE IF EXISTS test_data_types;
CREATE TABLE IF NOT EXISTS test_data_types(bool_field boolean, int_field int, float_field float, double_field double, text_field text, date_field date, time_field time, timestamp_field timestamp, uuid_field uuid PRIMARY KEY);
INSERT INTO test_data_types(bool_field, int_field, float_field, double_field, text_field, date_field, time_field, timestamp_field, uuid_field) VALUES(true, 29972944, 5.435, 2.359854, 'text!', '2019-08-23', '07:12:00', '2019-08-23 07:12:00-05', uuid());

DROP TABLE IF EXISTS test_index;
CREATE TABLE IF NOT EXISTS test_index(id int PRIMARY KEY, name varchar);
INSERT INTO test_index(id, name) VALUES(1, 'Sunday');
INSERT INTO test_index(id, name) VALUES(2, 'Monday');
INSERT INTO test_index(id, name) VALUES(3, 'Tuesday');
INSERT INTO test_index(id, name) VALUES(4, 'Wednesday');
INSERT INTO test_index(id, name) VALUES(5, 'Thursday');
INSERT INTO test_index(id, name) VALUES(6, 'Friday');
INSERT INTO test_index(id, name) VALUES(7, 'Saturday');

DROP TABLE IF EXISTS teacher;
CREATE TABLE IF NOT EXISTS teacher(id int, name text, email set<text>, coursenames list<text>, PRIMARY KEY(id));
INSERT INTO teacher(id, name, email, coursenames) VALUES(1, 'Hamilton', {'abc@gmail.com', 'xyz@gmail.com'}, ['Data Science', 'Neural Network']);

DROP TABLE IF EXISTS course;
CREATE TABLE IF NOT EXISTS course(id int, prerq map<text, int>, PRIMARY KEY(id));
INSERT INTO course(id, prerq) VALUES(1, {'DataScience' : 1, 'Neural Network' : 2});

DROP TABLE IF EXISTS user;
DROP TYPE IF EXISTS address;
DROP TYPE IF EXISTS phone;
CREATE TYPE phone (alias text, number text);
CREATE TYPE address (street text, ZIP int, phones set<frozen<phone>>);
CREATE TABLE user (id int PRIMARY KEY, addr frozen<address>, main_phone frozen<phone>);