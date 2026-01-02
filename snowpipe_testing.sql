--use role
use role accountadmin;

--create database
create or replace database snowpipe_dev;

--create table
create or replace table orders_data_lz(
    order_id int,
    product varchar(20),
    quantity int,
    order_status varchar(30),
    order_date date
);


--create a storage cloud integration in snowflake
-- integration means creating config based secure access
create or replace storage integration gcs_bucket_read_int
type = external_stage
storage_provider=gcs
enabled=true
storage_allowed_locations=('gcs://snowpipe_raw_data_ds/');


--command to drop any integration
--drop integration gcs_bucket_read_int

--Retrieve the cloud storage service account for your snowflake account
desc storage integration gcs_bucket_read_int;


--service account info for storage integration
-- k98530000@gcpuscentral1-1dfa.iam.gserviceaccount.com
-- -- snowflake_role
-- storage.buckets.list
-- storage.objects.get
-- storage.objects.list


-- a stage in snowflake refers to a location (internal or external)
-- where data files are uploaded, stored and prepared before being loaded into Snowflake tables
create or replace stage snowpipe_stage
url='gcs://snowpipe_raw_data_ds/'
storage_integration=gcs_bucket_read_int;


show stages;
-- SNOWPIPE_DEV.PUBLICSNOWPIPE_DEV.PUBLIC

list @snowpipe_stage;


--create PUB-SUB Topic named as gcs-to-pubsub-notification
-- then run below mentioned command from the google console Cloud Shell to setup create notification event 
-- gsutil notification create -t gcs-to-pubsub-notification -f json gs://snowpipe-raw-data-gds/
-- -- pubsub_snowflake_role
-- monitoring.timeSeries.list

--create notification integration 
create or replace notification integration notification_from_pubsub_int
 type = queue
 notification_provider = gcp_pubsub
 enabled = true
 gcp_pubsub_subscription_name =  'projects/my-project-69-481401/subscriptions/gcs-to-pubsub-notification-sub';

desc integration notification_from_pubsub_int;


--Service account fot Pub/Sub which needs to be whitelisted under Google Cloud IAM
-- -- ka8530000@gcpuscentral1-1dfa.iam.gserviceaccount.com
-- Pub/Sub Subscriber
-- pubsub_snowflake_role

--Create Snow pipe 
Create or replace pipe gcs_to_snowflake_pipe
auto_ingest = true
integration = notification_from_pubsub_int
as
copy into orders_data_lz
from @snowpipe_stage
file_format = (type='CSV');

--show pipe
show pipes;

--checking the status of the pipe 
select system$pipe_status('gcs_to_snowflake_pipe');


--check the history of the copy commands on a table
Select * 
from table(information_schema.copy_history(table_name=>'orders_data_lz', start_time=> dateadd(hours, -1, current_timestamp())));



select * from orders_data_lz


--stoping the snowpipe
ALTER PIPE gcs_to_snowflake_pipe SET PIPE_EXECUTION_PAUSED=true;

--terminating or deleting the snowpipe
-- drop pipe gcs_to_snowflake_pipe;
