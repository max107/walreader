drop table if exists simple_demo;
create table simple_demo (number int);
alter table simple_demo replica identity full;
select pg_create_logical_replication_slot('simple_demo', 'pgoutput');
drop publication if exists simple_demo;
create publication simple_demo for table simple_demo;