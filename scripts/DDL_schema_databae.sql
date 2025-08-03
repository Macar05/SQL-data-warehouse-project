/*
=====================================================
Creating database and schemas
=======================================
  script purpose:
  this script creates new database named 'Datawarehouse' after checking if exists  if it exists it will be dropped and recreated.
  and additionally the script sets up three scemas within the database 'bronze', 'silver' and 'gold'

warnoing:
  Running this script will drop the entire database 'datawarehouse' data base if exists.
  All data in the database will be deleted permanentaly, proceed with caution and ensure tou have proper backup before running.

*/



--create database
use master;
go


--Drop And recreate  'DataWarehouse' Database
if exists(select 1 from sys.databases where name='DataWarehouse')
begin
	alter database DataWarehouse set single_user with rollback immediate;
	drop database DataWarehouse;
end;
go

--- Cretae database 'DataWarehous'

create database DataWarehouse;

use DataWarehouse;

create schema bronze;

go

create schema silver;

go

create schema gold;

go
