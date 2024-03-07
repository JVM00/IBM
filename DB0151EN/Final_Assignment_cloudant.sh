#Export the json into your Cloudant Database

curl -XPOST $CLOUDANTURL/animal/_bulk_docs -Hcontent-type:application/json -d @animal.json

# Working with a Cloudant database

#Create Non-patitioned database named animal in IBM Cloudant
#Run the given command in the terminal to export the data of the “animal.json” file into your Cloudant database “animal”:

touch animal.json
sudo nano animal.json
cat animal.json

export CLOUDANTURL="https://apikey-v2-2395vwcnlwcp0gxgwdjXXXX25de-XXXX-XXXX-XXX-bluemix.cloudantnosqldb.appdomain.cloud"

curl -XPOST $CLOUDANTURL/animal/_bulk_docs -Hcontent-type:application/json -d @animal.json

#Create an index for the class key, on the animal database using the HTTP API

curl -X POST $CLOUDANTURL/animal/_index \
-H"Content-Type: application/json" \
-d'{
    "index": {
        "fields": ["class"]
    }
}'


#Write a query to find all animals belong to mammal class
<<comment

 {
    "selector": {
     "class": {
         "$eq": "mammals"
      }
    }
 }

comment

#Create an index for the diet key, on the animal database using the HTTP API

curl -X POST $CLOUDANTURL/animal/_index \
-H"Content-Type: application/json" \
-d'{
    "index": {
        "fields": ["diet"]
    }
}'

#Create an index for the _id key, on the animal database using the HTTP API

curl -X POST $CLOUDANTURL/animal/_index \
-H"Content-Type: application/json" \
-d'{
    "index": {
        "fields": ["_id"]
    }
}'

#Write a query to list only the class and diet keys for the panda animal using the HTTP API


 curl -X POST $CLOUDANTURL/animal/_find \
-H"Content-Type: application/json" \
-d'{ "selector":
        {          
	 "latin_name":"panda"
                
        },
   "fields": ["class", "diet"]
    }'

#Export the data from the animal database into a file named animal.json

couchexport --url $CLOUDANTURL --db animal --type jsonl > animal.json
