{
"BookingId" : 100,
"FirstName" : "William",
"LastName"  : "Kara",
"Location" : "Austin",
"OriginalPrice": 500,
"DiscountOffered": 10,
"BookingAmount": 450
}

{
"BookingId" : 101,
"FirstName" : "Peter",
"LastName"  : "Boso",
"Location" : "Florida",
"OriginalPrice": 500,
"DiscountOffered": 15,
"BookingAmount": 425,
"NoofRooms" : 2,
"Floor" : 2
}

{
"BookingId" : 102,
"FirstName" : "Chusko",
"LastName"  : "Gary",
"Location" : "San Jose",
"OriginalPrice": 500,
"DiscountOffered": 5,
"BookingAmount": 475,
"Floor" : 2,
"ApartmentType" : "Luxury"
}

{
"BookingId" : 103,
"FirstName" : "Elon",
"LastName"  : "Johny",
"Location" : "Texas",
"OriginalPrice": 500,
"DiscountOffered": 0,
"BookingAmount": 500,
"Interests" : ["SeaView","RareView"],
"Curreny": "USD"
}

{
"BookingId" : 104,
"FirstName" : "Brandon",
"LastName"  : "Jose",
"Location" : "Florida",
"OriginalPrice": 500,
"DiscountOffered": 5,
"BookingAmount": 475,
"NoofDays": 3,
"Airlines" : "Delta",
"Dropoff": "Yes"
}


SELECT * FROM c

SELECT * FROM c WHERE c.BookingId=100

SELECT BookingAmount FROM c WHERE c.BookingId=100