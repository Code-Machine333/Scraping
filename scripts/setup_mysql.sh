#!/bin/bash
# Linux/macOS script to setup MySQL for Cricket Database
# Run this script with appropriate MySQL privileges

echo "Setting up MySQL for Cricket Database..."
echo

# Check if MySQL is running
if ! systemctl is-active --quiet mysql 2>/dev/null && ! systemctl is-active --quiet mysqld 2>/dev/null; then
    echo "MySQL service not found or not running."
    echo
    echo "To install MySQL on Ubuntu/Debian:"
    echo "  sudo apt update && sudo apt install mysql-server"
    echo
    echo "To install MySQL on CentOS/RHEL:"
    echo "  sudo yum install mysql-server"
    echo
    echo "To install MySQL on macOS:"
    echo "  brew install mysql"
    echo
    exit 1
fi

# Start MySQL service if not running
if ! systemctl is-active --quiet mysql 2>/dev/null && ! systemctl is-active --quiet mysqld 2>/dev/null; then
    echo "Starting MySQL service..."
    if systemctl start mysql 2>/dev/null || systemctl start mysqld 2>/dev/null; then
        echo "MySQL service started."
    else
        echo "Failed to start MySQL service. Please check your MySQL installation."
        exit 1
    fi
fi

echo "MySQL service is running."
echo

# Run the SQL setup script
echo "Running database setup script..."
mysql -u root -p < "$(dirname "$0")/setup_mysql.sql"

if [ $? -eq 0 ]; then
    echo
    echo "Database setup completed successfully!"
    echo
    echo "Next steps:"
    echo "1. Update your .env file with the database credentials"
    echo "2. Run: make migrate"
    echo "3. Run: make etl"
    echo
else
    echo
    echo "Database setup failed. Please check the error messages above."
    echo
    exit 1
fi
