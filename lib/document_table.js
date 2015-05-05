var _ = require("underscore")._;
var assert = require("assert");
var util = require('util');
var Document = require("./document");
var Where = require("./where");
var ArgTypes = require("./arg_types");
var idGen = null;

exports.setGenid = function(_genId) {
  genid = _genId;
};

//Searching query for jsonb docs
exports.search = function(args, next){
  assert(args.keys && args.term, "Need the keys to use and the term string");

  //yuck full repetition here... fix me...
  if(!_.isArray(args.keys)){
    args.keys = [args.keys];
  };
  var tsv;
  if(args.keys.length === 1){
    tsv = util.format("(body ->> '%s')", args.keys[0])
  }else{
    var formattedKeys = [];
    _.each(args.keys, function(key){
      formattedKeys.push(util.format("(body ->> '%s')", key));
    });
    tsv= util.format("concat(%s)", formattedKeys.join(", ' ',"));
  }
  var sql = "select * from " + this.fullname + " where " + util.format("to_tsvector(%s)", tsv);
  sql+= " @@ to_tsquery($1);";

  this.executeDocQuery(sql, [args.term], next);
};

exports.insert = function(args, next) {
  this.save('insert', args, next);
}

exports.update = function() {
  var args, next;
  if (_.isString(arguments[0])) {
    args = _.extend(arguments[1], {id: arguments[0]});
    next = arguments[2];
  } else {
    args = arguments[0];
    next = arguments[1];
  }
  this.save('update', args, next);
}

exports.save = function(operation, args, next){
  assert(_.isObject(args), "Please pass in the document for saving as an object. Include the primary key for an UPDATE.");
  //see if the args contains a PK
  var self = this;
  var sql, params = [];
  var pkName = this.primaryKeyName();
  var pkVal = args[pkName];
  if (!pkVal) {
    pkVal = genid();
  }

  //just in case
  delete args[pkName];
  params.push(pkVal);

  if (operation === 'update') {
    if (args.$set) {
      var path = _.keys(args.$set)[0];
      var val = args.$set[path];
      params.push(path, JSON.stringify(val));
      sql = util.format("select json_set('%s', $1, $2, $3::JSON);",
        this.fullname);
    } else if (args.$insert) {
      var path = _.keys(args.$insert)[0];
      var val = args.$insert[path];
      params.push(path, JSON.stringify(val));
      sql = util.format("select json_insert('%s', $1, $2, $3::JSON);",
        this.fullname);
    } else if (args.$push) {
      var path = _.keys(args.$push)[0];
      var val = args.$push[path];
      params.push(path, JSON.stringify(val));
      sql = util.format("select json_push('%s', $1, $2, $3::JSON);",
        this.fullname);
    } else if (args.$remove) {
      var path = args.$remove;
      params.push(path);
      sql = util.format("select json_remove('%s', $1, $2);", this.fullname);
    } else {
      params.push(JSON.stringify(args));
      sql = util.format("update %s set body = $2 where %s = $1 returning *;",
        this.fullname, pkName);
    }
  } else if (operation === 'insert') {
    params.push(JSON.stringify(args));
    sql = "insert into " + this.fullname + "(id, body) values($1, $2) returning *;"
  } else {
    throw("Operation must be either 'update' or 'insert'");
  }
  this.executeDocQuery(sql, params, {single : true}, next)
};

exports.find = function(){
  var args = ArgTypes.findArgs(arguments);
  if(_.isFunction(args.conditions)){
    // all we're given is the callback:
    args.next = args.conditions;
  }

  //set default options
  args.options.order || (args.options.order = util.format('"%s"', this.pk));
  args.options.limit || (args.options.limit = "1000");
  args.options.offset || (args.options.offset = "0");
  args.options.columns || (args.options.columns = "*");

  order = " order by " + args.options.order;
  limit = " limit " + args.options.limit;
  offset = " offset " + args.options.offset;

  var sql = util.format("select id, body from %s", this.fullname);
  if (_.isEmpty(args.conditions)) {
    // Find all
    sql += order + limit + offset;
    this.executeDocQuery(sql, [], args.options, args.next);
  } else {
    // Set up the WHERE statement:
    var where = this.getWhereForDoc(args.conditions);
    sql += where.where + order + limit + offset;
    if(where.pkQuery) {
      this.executeDocQuery(sql, where.params, {single:true}, args.next);
    } else {
      this.executeDocQuery(sql, where.params,args.options, args.next);
    }
  }
};

this.getWhereForDoc = function(conditions) {
  var where = {};
  if(_.isFunction(conditions) || conditions == "*") {
    // no crtieria provided - treat like select *
    where.where = "";
    where.params = [];
    where.pkQuery = false;
    return where;
  }

  if(_.isString(conditions)){
    //assume it's a search on ID
    conditions = {id : conditions};
  };

  if(_.isObject(conditions)) {
    var keys = _.keys(conditions);
    if(keys.length = 1) {
      var operator = keys[0].match("<=|>=|!=|<>|=|<|>");
      var property = keys[0].replace(operator, "").trim();
      if(property == this.primaryKeyName()) {
        // this is a query against the PK...we can use the
        // plain old table "where" builder:
        where = Where.forTable(conditions);
        where.pkQuery = true;
        if(operator != null && operator != "=") {
          // someone passed an operator as part of the key, such as "id >" : 1
          // this will likely return more than one result:
          where.pkQuery = false;
        }
      } else {
        var where = Where.forDocument(conditions);
        where.pkQuery = false;
      }
    } else {
      var where = Where.forDocument(conditions);
      where.pkQuery = false;
    }
  }
  return where;
};

this.executeDocQuery = function() {
  var args = ArgTypes.queryArgs(arguments);
  var doc = {};
  this.db.query(args.sql, args.params, args.options, function(err,res){
    if(err){
      args.next(err,null);
    }else{
      //only return one result if single is sent in
      if(args.options.single){
        doc = Document.formatDocument(res);
      }else{
        doc = Document.formatArray(res);
      }
      args.next(null,doc);
    }
  });
};

