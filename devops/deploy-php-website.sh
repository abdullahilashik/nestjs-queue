#!/bin/bash

# Update before installing any packages
sudo apt update -y
# Bash script to install mysql, database scirpts, php and deploy a php website on ubuntu server
sudo apt  install -y mariadb-server

# Create cofnir database file
cat > configure-db.sql <<-EOF
CREATE DATABASE ecomdb;
CREATE USER 'ecomuser'@'localhost' IDENTIFIED BY 'ecompass';
GRANT ALL PRIVILEGES ON ecomdb.* TO 'ecomuser'@'localhost';
FLUSH PRIVILEGES;
EOF


# Run the conf db file
sudo mysql < configure-db.sql

# Insert data file 

cat > db-load-script.sql <<-EOF
USE ecomdb;
CREATE TABLE products (id mediumint(8) unsigned NOT NULL auto_increment,Name varchar(255) default NULL,Price varchar(255) default NULL, ImageUrl varchar(255) default NULL,PRIMARY KEY (id)) AUTO_INCREMENT=1;

INSERT INTO products (Name,Price,ImageUrl) VALUES ("Apple","100","c-1.png"),("Orange","200","c-2.png"),("Lemon","300","c-3.png"),("Potato","50","c-5.png"),("Fish","90","c-6.png"),("Chicken","20","c-7.png"),("Beef","80","c-8.png"),("Apple","150","c-4.png");

EOF

# Insert the file into database

sudo mysql < db-load-script.sql

# Install required packages for php website

sudo apt install -y php libapache2-mod-php php-mysql apache2 unzip zip git

# Download the php website from github
sudo rm -rf /var/www/html/*
sudo git clone https://github.com/rajivsiddiqui/project-7-shell-script.git /var/www/html

#

