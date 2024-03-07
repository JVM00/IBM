
// Import animal.json into mongodb server into a database named wildlife and a collection named anima
// mongoimport -u root -p XXXX! --authenticationDatabase admin --db wildlife --collection animal --file animal.json


// Write a mongodb query to find the class which has the greatest number of animals in the wildlife database.

//mongosh -u jvm -p XXXXX! --authenticationDatabase admin local
//use wildlife;
//show collections

db.animal.aggregate(
[
    {
        "$group":{
		"_id":"$class","count":{"$count": {}}
		 }
    },
   {
       "$sort":{"count":-1}
   },
   {
    "$limit":1
   }

])

//Write a query to print the _id and diet of all animals that belong to the bird class

db.animal.find(
    {"class":"Bird"},
    {
      "_id": 1,
      "diet": 1
    })

//Export the fields _id, class, diet, latin_name from the animal collection into a file named wild_animal.csv

//mongoexport -u root -p XXXXX --authenticationDatabase admin --db wildlife --collection animal --out wild_animal.csv --type=csv --fields _id,class,diet,latin_name

