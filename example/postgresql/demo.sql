drop table if exists users;
create table users (id serial primary key, name text not null);
alter table users replica identity full;
select pg_drop_replication_slot('cdc_slot');
select pg_create_logical_replication_slot('cdc_slot', 'pgoutput');
drop publication if exists cdc_publication;
create publication cdc_publication for table users;
