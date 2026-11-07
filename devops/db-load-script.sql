USE ecomdb;
CREATE TABLE products (id mediumint(8) unsigned NOT NULL auto_increment,Name varchar(255) default NULL,Price varchar(255) default NULL, ImageUrl varchar(255) default NULL,PRIMARY KEY (id)) AUTO_INCREMENT=1;

INSERT INTO products (Name,Price,ImageUrl) VALUES ("Apple","100","c-1.png"),("Orange","200","c-2.png"),("Lemon","300","c-3.png"),("Potato","50","c-5.png"),("Fish","90","c-6.png"),("Chicken","20","c-7.png"),("Beef","80","c-8.png"),("Apple","150","c-4.png");

