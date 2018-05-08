const mongodb = require('mongodb');
const MongoClient = mongodb.MongoClient;
const Mongo = function Mongo(documents){

  const url = 'mongodb://localhost:27017/migration';
  const connection = function connection(){
    MongoClient.connect(url,(err,db) =>{
      if(err) return process.exit(1);
      insertDocuments(db, result =>{
        db.close();
      })
    });
  }

  const insertDocuments = function insertDocuments(db,callback){
    const collection = db.collection('edx-migration');
    collection.insert(documents, (error,result)=>{
      if(error) return process.exit(1);
      callback(result);
    });
  };

  return{connection:connection};
};

module.exports = Mongo;
