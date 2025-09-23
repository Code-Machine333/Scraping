@echo off
REM Windows batch script to setup MySQL for Cricket Database
REM Run this script as Administrator or with appropriate MySQL privileges

echo Setting up MySQL for Cricket Database...
echo.

REM Check if MySQL is running
sc query mysql80 >nul 2>&1
if %errorlevel% neq 0 (
    echo MySQL service not found. Please ensure MySQL 8.0+ is installed.
    echo.
    echo To install MySQL on Windows:
    echo 1. Download MySQL Installer from https://dev.mysql.com/downloads/installer/
    echo 2. Run the installer and select "MySQL Server" and "MySQL Workbench"
    echo 3. Set root password and start MySQL service
    echo.
    pause
    exit /b 1
)

REM Start MySQL service if not running
sc query mysql80 | find "RUNNING" >nul
if %errorlevel% neq 0 (
    echo Starting MySQL service...
    net start mysql80
    if %errorlevel% neq 0 (
        echo Failed to start MySQL service. Please check your MySQL installation.
        pause
        exit /b 1
    )
)

echo MySQL service is running.
echo.

REM Run the SQL setup script
echo Running database setup script...
mysql -u root -p < "%~dp0setup_mysql.sql"

if %errorlevel% equ 0 (
    echo.
    echo Database setup completed successfully!
    echo.
    echo Next steps:
    echo 1. Update your .env file with the database credentials
    echo 2. Run: make migrate
    echo 3. Run: make etl
    echo.
) else (
    echo.
    echo Database setup failed. Please check the error messages above.
    echo.
)

pause
