--CREATE DATABASE
CREATE DATABASE myBookingsDB;

--USE DATABASE
USE myBookingsDB;

--CREATE TABLE
CREATE TABLE Bookings (
BookingID INT NOT NULL UNIQUE, 
FirstName VARCHAR(255) , 
LastName VARCHAR (255) , 
City VARCHAR(255) DEFAULT 'Texas' , 
OriginalPrice FLOAT,
DiscountOffered FLOAT ,
BookingAmount  FLOAT,
PRIMARY KEY (BookingID),
CHECK (DiscountOffered<20) 
);


CREATE TABLE SalesbyBookings (
    SalesID INT NOT NULL,
    BookingID INT NOT NULL,
    RoomType VARCHAR(255),
    NoofRooms INT,
    FloorType INT,
    NoofPeopleToOccupy INT,
    
    PRIMARY KEY (SalesID),
    FOREIGN KEY (BookingID) REFERENCES Bookings(BookingID));



--INSERT DATA INTO BOOKINGS TABLE
INSERT INTO Bookings VALUES(100,'William','Kara','Austin','500','10','450');
INSERT INTO Bookings VALUES(101,'Peter','Boso','Florida','500','15','425');
INSERT INTO Bookings VALUES(102,'Chusko','Gary','San Jose','500','5','475');
INSERT INTO Bookings VALUES(103,'Elon','Johny','Texas','500','0',NULL);
INSERT INTO Bookings VALUES(104,'Brandon','Jose','Florida','500','5','475');


--INSERT DATA INTO SALESBYBOOKINGS TABLE
INSERT INTO SalesbyBookings VALUES(1100,100,'3BHK',3,1,3);
INSERT INTO SalesbyBookings VALUES(1101,101,'1BHK',1,3,1);
INSERT INTO SalesbyBookings  VALUES(1102,102,'2BHK',2,2,2);
INSERT INTO SalesbyBookings  VALUES(1103,103,'3BHK',3,NULL,2);

--QUERY DATA FROM BOOKINGS TABLE
SELECT * FROM Bookings


--QUERY DATA FROM SALESBYBOOKINGS TABLE
SELECT * FROM SalesbyBookings


--ARTHMETIC OPERATIONS

SELECT BookingID,(BookingAmount+50) AS NewBookingAmount FROM Bookings

SELECT BookingID,(BookingAmount-50) AS NewBookingAmount FROM Bookings

SELECT BookingID,(BookingAmount*50) AS NewBookingAmount FROM Bookings

SELECT BookingID,(BookingAmount/50) AS NewBookingAmount FROM Bookings

--ALTER TABLES BOOKINGS TO ADD/DROP
ALTER TABLE Bookings ADD RoomType VARCHAR(255);
ALTER TABLE Bookings DROP COLUMN RoomType;
ALTER TABLE Bookings ADD RoomType INT;


--QUERY DATA FROM BOOKINGS WITH BOOKINGID && BOOKING AMOUNT
SELECT BookingID,BookingAmount FROM Bookings;


--QUERY DATA FROM BOOKINGS WITH BOOKINGID && BOOKING AMOUNT ORDERBY DiscountOffered
SELECT BookingID,BookingAmount FROM Bookings ORDER BY DiscountOffered;

--TO COUNT THE NUMBER OF BOOKINGS
SELECT COUNT(BookingID) FROM Bookings;

--TOTAL BOOKING AMOUNT
SELECT * FROM Bookings;
SELECT SUM(BookingAmount) as Total_BookingAmount FROM  Bookings ;

--AVG BOOKING AMOUNT
SELECT AVG(BookingAmount) as AVG_BookingAmount FROM  Bookings ;

--MAX AND MIN BOOKING AMOUNT
SELECT MAX(BookingAmount)as Max_BookingAmount,MIN(BookingAmount) as Min_BookingAmount FROM Bookings;

--BOOKINGS WITH DISCOUNT % OFFERED IN BETWEEN 5 & 10
SELECT * FROM Bookings;
SELECT BookingID,BookingAmount FROM Bookings WHERE DiscountOffered BETWEEN 5 and 10;

-- INNER JOINS
SELECT * FROM Bookings;
SELECT * FROM SalesbyBookings;

SELECT Bookings.BookingID,Bookings.FirstName,Bookings.LastName,SalesbyBookings.BookingID,SalesbyBookings.NoofPeopleToOccupy

FROM

Bookings INNER JOIN SalesbyBookings ON Bookings.BookingID=SalesbyBookings.BookingID

--LEFT JOIN
SELECT  * FROM 
Bookings LEFT JOIN SalesbyBookings ON Bookings.BookingID=SalesbyBookings.BookingID

--RIGHT JOIN
SELECT  * FROM 
Bookings RIGHT JOIN SalesbyBookings ON Bookings.BookingID=SalesbyBookings.BookingID

--FULL JOIN
SELECT  * FROM 
Bookings FULL JOIN SalesbyBookings ON Bookings.BookingID=SalesbyBookings.BookingID

--STORED PROCEDURES
CREATE PROCEDURE TotalSales_Stored_Procedure
AS 
SELECT BookingID,BookingAmount from Bookings
GO;

EXEC TotalSales_Stored_Procedure

--ISNULL FUNCTIONS
SELECT BookingID,ISNULL(BookingAmount,0) FROM Bookings;

--GROUP BY
SELECT COUNT(NoofRooms),FloorType FROM SalesbyBookings GROUP BY FloorType;
SELECT DiscountOffered,COUNT(DiscountOffered) FROM Bookings GROUP BY DiscountOffered;

--CASE STATEMENTS
SELECT 
    BookingID,
    SalesID,
    RoomType,
    CASE NoofPeopleToOccupy         
        WHEN 1 THEN 'Single'
        WHEN 2 THEN 'Double'
        WHEN 3 THEN 'Triple'
    END
    
    AS SuggestedRoomType

FROM SalesbyBookings;

--SQL EXISTS SELECT 
SELECT BookingID,FirstName,LastName FROM Bookings 
WHERE EXISTS (SELECT DiscountOffered FROM Bookings WHERE DiscountOffered=15)

--DROP TABLES
DROP TABLE SalesbyBookings
DROP TABLE Bookings

--DROP DATABASE
DROP mybookingsDB







