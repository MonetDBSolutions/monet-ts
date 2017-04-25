SET SCHEMA timetrails;

drop TABLE cpu;

create table cpu (
     time TIMESTAMP,
     arch STRING,
     datacenter STRING,
     hostname STRING,
     os STRING,
     rack STRING,
     region STRING,
     service STRING,
     service_environment STRING,
     service_version STRING,
     team STRING,
     usage_guest FLOAT,
     usage_guest_nice FLOAT,
     usage_idle FLOAT,
     usage_iowait FLOAT,
     usage_irq FLOAT,
     usage_nice FLOAT,
     usage_softirq FLOAT,
     usage_steal FLOAT,
     usage_system FLOAT,
     usage_user FLOAT,
PRIMARY KEY(time,arch,datacenter,hostname,os,rack,region,service_environment,service_version,team));

COPY INTO cpu from '/Users/svetlin/MyWorkspace/monet-ts/csv/cpu.csv' DELIMITERS ',','\n';