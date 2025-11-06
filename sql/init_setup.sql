USE [master];
GO

/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'NHS_A_E_Warehouse' after checking if it already exists. 
    If the database exists, it is not created again. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
*/

-- Create the single Data Warehouse Database
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'NHS_A_E_Warehouse')
BEGIN
    CREATE DATABASE [NHS_A_E_Warehouse];
END
GO

USE [NHS_A_E_Warehouse];
GO

-- Create the Schemas for each layer
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'bronze')
BEGIN
    EXEC('CREATE SCHEMA [bronze]');
END
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'silver')
BEGIN
    EXEC('CREATE SCHEMA [silver]');
END
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'gold')
BEGIN
    EXEC('CREATE SCHEMA [gold]');
END
GO