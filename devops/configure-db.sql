CREATE DATABASE ecomdb;
CREATE USER 'ecomuser'@'localhost' IDENTIFIED BY 'ecompass';
GRANT ALL PRIVILEGES ON ecomdb.* TO 'ecomuser'@'localhost';
FLUSH PRIVILEGES;
