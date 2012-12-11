create table customer_demographics_stg (
  cd_demo_sk int,
  cd_gender string,
  cd_marital_status string,
  cd_education_status string,
  cd_purchase_estimate int,
  cd_credit_rating string,
  cd_dep_count int,
  cd_dep_employed_count int,
  cd_dep_college_count int
) row format delimited fields terminated by '|';

load data local inpath '/home/horton/customer_demographics.dat' 
  into table customer_demographics_stg;

add jar /home/horton/share/orc-1.0-SNAPSHOT.jar;
add jar /home/horton/share/protobuf-java-2.4.1.jar;

create table customer_demographics (
  cd_demo_sk int,
  cd_gender string,
  cd_marital_status string,
  cd_education_status string,
  cd_purchase_estimate int,
  cd_credit_rating string,
  cd_dep_count int,
  cd_dep_employed_count int,
  cd_dep_college_count int
) row format serde 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
  stored as 
    inputformat 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
    outputformat 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';

insert overwrite table customer_demographics 
  select * from customer_demographics_stg;
