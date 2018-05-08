Module 3 Assignment Lab: MongoDB Migration Node Script

The assignment lab for this module is to build a migration script to move data from one MongoDB database to another. Unlike the tutorial labs, there will be no step by step instructions but all of the information you need to know should be in the modules. Please attempt to solve the assignment lab on your own before viewing the solution.

MongoDB Migration Node Script
You work at a Bitcoin exchange company. For some reason, the customer data is missing its full address. You have customer data only with the street address (building number and street name) but no city, state, country. Also, phone numbers are missing too. Luckily, your friends were able to restore the address information from a backup replica of a MongoDB instance. You need to write a migration/restoration script which will merge the data from the two sources.

You have millions of records so you need to create a script which can run queries to the database in parallel. You don't know what is the optimal number of customers to insert into a database at a time so you need to write the program to allow for a variable number of documents to be able to be updated at once. This will help to determine if it's better to update in groups of 10, 50 or maybe 500 at a time.

You are provided with two sample JSON files customer-data.json and customer-address-data.json which contain 1000 documents/objects (remember, the real data will have 1000000+ objects). Assume that the order of the objects in each file correlates to objects in the other file.


Run it in this way
$ node server.js <limit>

for example : node server.js 10

Mongodb logic is in mongo.js file, I used async.parallel to run the tasks in parallel
