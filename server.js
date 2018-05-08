const path = require('path');
const fs = require('fs');
const async = require('async');
const Mongo = require('./mongo.js');

const Migrate = function Migrate(){

  const process = function process(limit = 10){
    console.log(`The chunks size will be ${limit} records`);
    if( !resourcesExist()) return console.log('One or both of the resourses is missing');

    const dataFile = path.join(__dirname,'resources','m3-customer-data.json');
    const addressFile = path.join(__dirname,'resources','m3-customer-address-data.json');

    const dataPromise = new Promise( (resolve,reject)=>{
      fs.readFile(dataFile,'utf8', (err,data)=>{
        if(err) reject(err);
        return resolve(JSON.parse(data));
      });
    });

    dataPromise.then(data =>{
      return new Promise( (resolve,reject)=>{
        fs.readFile(addressFile,'utf8', (err,address)=>{
          if(err) reject(err);
          address= JSON.parse(address);
          return resolve({data,address});
        });
      });
    }).then(function(result){
      return generateTasks(result);

    }).then(function(tasks){
      console.log(`Number of parallel tasks : ${tasks.length}`);
      async.parallel(tasks, (error,results)=>{
        if(error) console.log(error);
      });
    }).catch(error => {
      console.log(error);
    });



  };

  const resourcesExist = function resourcesExist(){
    if(!fs.existsSync(path.join(__dirname,'resources','m3-customer-data.json'))){
      return false;
    }
    if(! fs.existsSync(path.join(__dirname,'resources','m3-customer-address-data.json'))){
      return false;
    }
    return true;
  };


  const generateTasks = function generateTasks(results){
    const data = results.data;
    const address = results.address;


    //blocking code
    const chunks = [];
    const merged = [];
    let tempArray = [];
    let counter = 1;

    data.forEach( (element,i)=>{

      merged.push({...data[i],...address[i]});
      tempArray.push({...data[i],...address[i]});

      if(counter === parseInt(limit)){
        chunks.push([...tempArray]);
        tempArray = [];
        counter = 0;
      }
      counter++;
    });

    if(counter != 1){
      counter--;
      chunks.push(merged.slice((merged.length -1) - counter, (merged.length - 1)));
    }
    const tasks = [];
    chunks.forEach(chunk=>{
      const mongo = Mongo(chunk);
      tasks.push(mongo.connection);
    });
    return Promise.resolve(tasks);
  };

  return { process };
};


const limit = process.argv[2];
const start = Migrate();
start.process(limit);
