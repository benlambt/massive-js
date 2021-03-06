var Runner = require("./lib/runner");
var _ = require("underscore")._;
var fs = require("fs");
var Table = require("./lib/table");
var util = require("util");
var assert = require("assert");
var Document = require("./lib/document");
var ArgTypes = require("./lib/arg_types");
var Args = require("args-js");
var path = require("path");
var deasync = require('deasync');

var self;

var Massive = function(args){

  this.scriptsDir = args.scripts || process.cwd() + "/db";

  var runner = new Runner(args.connectionString);
  _.extend(this,runner);

  this.tables = [];
  this.queryFiles = [];
  this.schemas = [];
  this.functions = [];

  if(args.whitelist) {
    this.whitelist = this.getTableFilter(args.whitelist);
  } else {
    this.allowedSchemas = this.getSchemaFilter(args.schema);
    this.blacklist = this.getTableFilter(args.blacklist);
    this.exceptions = this.getTableFilter(args.exceptions);
  }
}

Massive.prototype.getSchemaFilter = function(allowedSchemas) {
  // an empty string will cause all schema to be loaded by default:
  var result = '';
  if(allowedSchemas === 'all' || allowedSchemas === '*') {
    // Do nothing else. Leave the default empty string:
    allowedSchemas = null;
  }
  if(allowedSchemas) {
    // there is a value of some sort other than our acceptable defaults:
    if(_.isString(allowedSchemas)) {
      // a string works. If comma-delimited, so much the better, we're done:
      result = allowedSchemas;
    } else {
      if(!_.isArray(allowedSchemas)) {
        throw("Specify allowed schemas using either a commma-delimited string or an array of strings");
      }
      // create a comma-delimited string:
      result = allowedSchemas.join(", ");
    }
  }
  return result;
};

Massive.prototype.getTableFilter = function(filter) {
  // an empty string will cause all schema to be loaded by default:
  var result = '';
  if(filter) {
    // there is a value of some sort other than our acceptable defaults:
    if(_.isString(filter)) {
      // a string works. If comma-delimited, so much the better, we're done:
      result = filter;
    } else {
      if(!_.isArray(filter)) {
        throw("Specify filter patterns using either a commma-delimited string or an array of strings");
      }
      // create a comma-delimited string:
      result = filter.join(", ");
    }
  }
  return result;
};

Massive.prototype.run = function(){
  var args = ArgTypes.queryArgs(arguments);
  this.query(args);
}

Massive.prototype.loadQueries = function() {
  walkSqlFiles(this,this.scriptsDir);
};


Massive.prototype.loadTables = function(next){
  var tableSql = __dirname + "/lib/scripts/tables.sql";
  var parameters = [this.allowedSchemas, this.blacklist, this.exceptions];
  var self = this;

  // ONLY allow whitelisted items:
  if(this.whitelist) {
    tableSql = __dirname + "/lib/scripts/whitelist.sql";
    var parameters = [this.whitelist]
  }
  this.executeSqlFile({file : tableSql, params: parameters}, function(err,tables){
    if(err){
      next(err,null);
    }else{
      _.each(tables, function(table){
        var _table = new Table({
          schema : table.schema,
          name : table.name,
          pk : table.pk,
          db : self
        });
        // This refactoring appears to work well:
        MapTableToNamespace(_table);
      });
      next(null,self);
    }
  });
}

Massive.prototype.insert = function(collection, doc, next) {
  this.save('insert', collection, doc, next);
}

Massive.prototype.update = function(collection, doc, next) {
  this.save('update', collection, doc, next);
}

Massive.prototype.save = function(operation, collection, doc, next){
  var self = this;

  collection = this.readCollection(collection);

  if(collection.table) {
    collection.table.save(operation, doc, next);
  } else {
    this.createDocTable(collection, function() {
      // recurse
      self.save(operation, collection.path, doc, next);
    });
  }
};

Massive.prototype.createDocTable = function (collection, next) {
  if (_.isString(collection)) {
    collection = this.readCollection(collection);
  }

  var _table = new Table({
    schema : collection.schemaName,
    pk : "id",
    name : collection.tableName,
    db : self
  });

  // Create the table in the back end:
  var sql = this.documentTableSql(collection.path);
  this.query(sql, function(err, res) {
    if (err) {
      next(err, null);
    } else {
      MapTableToNamespace(_table);
      next();
    }
  });
}

Massive.prototype.readCollection = function (collection) {
  // default is public. Table constructor knows what to do if 'public' is used as the schema name:
  var schemaName = "public";
  var tableName = collection;
  var potentialTable = null;

    // is the collection namespace delimited?
  var splits = collection.split(".");
  if(splits.length > 1) {
    // uh oh. Someone specified a schema name:
    schemaName = splits[0];
    tableName = splits[1];
    potentialTable = self[schemaName][tableName];
  } else {
    potentialTable = self[tableName];
  }
  return {
    schemaName: schemaName,
    tableName: tableName,
    table: potentialTable,
    path: collection
  };
};

var MapTableToNamespace = function(table) {
  var db = table.db;
  if(table.schema !== "public") {
    schemaName = table.schema;
    // is this schema already attached?
    if(!db[schemaName]) {
      // if not, then bolt it on:
      db[schemaName] = {};
    }
    // attach the table to the schema:
    db[schemaName][table.name] = table;
    db.tables.push(table);
  } else {
    //it's public - just pin table to the root to namespace
    db[table.name] = table;
    db.tables.push(table);
  }
}

Massive.prototype.documentTableSql = function(tableName){
  var docSqlFile = __dirname + "/lib/scripts/create_document_table.sql";
  var sql = fs.readFileSync(docSqlFile, {encoding: 'utf-8'});

  var indexName = tableName.replace(".", "_");
  sql = util.format(sql, tableName, indexName, tableName);
  return sql;
};

Massive.prototype.loadSprocs = function(next) {
  var sprocFile = __dirname + "/lib/scripts/json_sprocs.sql";
  var sql = fs.readFileSync(sprocFile, {encoding: 'utf-8'});
  this.query(sql, next);
};

//A recursive directory walker that would love to be refactored
var walkSqlFiles = function(rootObject, rootDir){
  var dirs;
  try {
    dirs = fs.readdirSync(rootDir);
  } catch (ex) {
     return;
  }

  //loop the directories found
  _.each(dirs, function(item){

    //parsing with path is a friendly way to get info about this dir or file
    var ext = path.extname(item);
    var name = path.basename(item, ext);

    //is this a SQL file?
    if(ext === ".sql"){

      //why yes it is! Build the abspath so we can read the file
      var filePath = path.join(rootDir,item);

      //pull in the SQL - don't worry this only happens once, when
      //massive is loaded using connect()
      var sql = fs.readFileSync(filePath, {encoding : "utf-8"});

      //set a property on our root object, and grab a handy variable reference:
      var newProperty = assignScriptAsFunction(rootObject, name);

      //I don't know what I'm doing, but it works
      newProperty.sql = sql;
      newProperty.db = self;
      newProperty.filePath = filePath;
      self.queryFiles.push(newProperty);

    }else if(ext !== ''){
      //ignore it
    }else{

      //this is a directory so shift things and move on down
      //set a property on our root object, then use *that*
      //as the root in the next call
      rootObject[name] = {};

      //set the path to walk so we have a correct root directory
      var pathToWalk = path.join(rootDir,item);

      //recursive call - do it all again
      walkSqlFiles(rootObject[name],pathToWalk);
    }
  });
}

Massive.prototype.loadFunctions = function(next){
  var functionSql = __dirname + "/lib/scripts/functions.sql";
  this.executeSqlFile({file : functionSql}, function(err,functions){
    if(err){
      next(err,null);
    }else{
      _.each(functions, function(fn){
        var schema = fn.schema;
        var sql;
        var params = [];

        for(var i = 1;i<=fn.param_count;i++){
          params.push("$" + i);
        }

        var newFn, pushOnTo
        if(schema !== "public"){
          self[schema] || (self[schema] =  {});
          newFn = assignScriptAsFunction(self[schema], fn.name);
          sql = util.format("select * from %s.%s", schema, fn.name);
          self[schema][fn.name] = newFn;
        }else{
          sql = "select * from " + fn.name;
          newFn= assignScriptAsFunction(self, fn.name);
          self[fn.name] = newFn;
        }

        sql+="(" + params.join(",") + ")";
        newFn.sql = sql;
        newFn.db = self;
        self.functions.push(newFn);
      });
      next(null,self);
    }
  });
};

//it's less congested now...
var assignScriptAsFunction = function (rootObject, propertyName) {
   rootObject[propertyName] = function(args, next) {
    args || (args = {});
    //if args is a function, it's our callback
    if(_.isFunction(args)){
      next = args;
      //set args to an empty array
      args = [];
    }
    //JA - use closure to assign stuff from properties before they are invented
    //(sorta, I think...):
    var sql = rootObject[propertyName].sql;
    var db = rootObject[propertyName].db;
    var params = _.isArray(args) ? args : [args];

    //execute the query on invocation
    db.query(sql,params,{}, next);
  }
  return rootObject[propertyName];
}

//connects Massive to the DB
exports.connect = function(args, next){
  assert((args.connectionString || args.db), "Need a connectionString or db (name of database on localhost) at the very least.");

  //override if there's a db name passed in
  if(args.db){
    args.connectionString = "postgres://localhost/"+args.db;
  }

  var massive = new  Massive(args);

  //load up the tables, queries, and commands
  massive.loadTables(function(err,db){
    self = db;
    massive.loadFunctions(function(err,db){
      assert(!err, err);
      db.loadSprocs(function(err) {
        assert(!err, err);
        //synchronous
        db.loadQueries();
        next(null,db);
      });
    });
  });
};

exports.loadSync = function(args) {
  var done = false;
  this.connect(args, function (err, res) {
    result = res;
    done = true;
  });
  while(!done) {
    deasync.runLoopOnce();
  }
  return result;
}

exports.config = function(opts) {
  Table.config(opts);
};

exports.Table = Table;
