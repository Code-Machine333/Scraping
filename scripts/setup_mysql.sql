-- MySQL Setup Script for Cricket Database
-- Run this script as root or a user with CREATE DATABASE privileges

-- Create the database
CREATE DATABASE IF NOT EXISTS cricket_db 
  CHARACTER SET utf8mb4 
  COLLATE utf8mb4_unicode_ci;

-- Create the user
CREATE USER IF NOT EXISTS 'cricket_user'@'localhost' 
  IDENTIFIED BY 'cricket_password';

-- Grant privileges
GRANT ALL PRIVILEGES ON cricket_db.* TO 'cricket_user'@'localhost';

-- Create user for remote access (optional)
CREATE USER IF NOT EXISTS 'cricket_user'@'%' 
  IDENTIFIED BY 'cricket_password';
GRANT ALL PRIVILEGES ON cricket_db.* TO 'cricket_user'@'%';

-- Flush privileges to ensure changes take effect
FLUSH PRIVILEGES;

-- Show the created database and user
SHOW DATABASES LIKE 'cricket_db';
SELECT User, Host FROM mysql.user WHERE User = 'cricket_user';

-- Display connection information
SELECT 'Database setup complete!' as status;
SELECT 'Database: cricket_db' as info;
SELECT 'User: cricket_user' as info;
SELECT 'Host: localhost' as info;
SELECT 'Port: 3306' as info;
