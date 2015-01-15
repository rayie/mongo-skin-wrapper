var mongoskin = require('mongoskin');
var ObjectID = require('mongodb').ObjectID;
var util = require('util');
var l = console.log;
module.exports = function( ){

  this.cursor_ref = {};
  this.cursors_created = 0;
  

  var self = this;
  self.db = null;

  this.coll = function(col_name){
    return self.db.collection(col_name);
  }

  /*
    allows passing in hex str representation as _id values in criteria like this:
  */
  this._resolve_ids = function(crit){

    // passing in a single native _id as a string { _id: { $oid: "5093402323...."} }   (how pymongo  does it)
    if( "object" === typeof crit._id){
      if( "string" === typeof crit._id.$oid){
        crit._id = new ObjectID(crit._id.$oid);
        return crit;
      }
    }

    // passing in a single native _id as a string { $oid: "5093402323...." }  
    if( "string" === typeof crit.$oid){ 
      crit._id = new ObjectID(crit.$oid.toString());
      delete crit.$oid;
      return crit;
    }

    // passing in an array of string _ids { $oids:  ["5093402323....", "409322323...."] } 
    if( "object" === typeof crit.$oids ) {
      crit._id = { $in: [ ] };
      crit.$oids.forEach(function(oid){ 
        switch(typeof oid){
          case "string":
            crit._id.$in.push(new ObjectID(oid)); 
            break;
          case "object":
            if ( ObjectID.isValid(oid) )
              crit._id.$in.push(oid); 
            else
              crit._id.$in.push(ObjectID.createFromHexString(oid.toString())); 
            break;
        }
      });
      delete crit.$oids;
      return crit;
    }
    return crit;  //_id must be a non native value
  }

  /*  
    URIs are of the form [BASEURL]/db_name/collection_name/_command  

  */
  this.save = function(doc, collection, callback){
    self.db.collection(collection).save(
      doc,
      callback
    );
  }


  // the collumn we need a seq id for, n: how many we need
  this._fetch_seq_ids = function( coll_name, n , callback){
    l("attempting fetch of seq_ids",coll_name,n);
    self.db.collection("seq_ids").findAndModify(
      { coll_name: coll_name }, 
      { }, 
      { $inc: { "last_id": n } },
      { },
      function( err, result, ur ){
        l("result from findAndModify of seq_ids", err, result, ur);
        if ( err ){
          l("got findAndModify error:", err);
        }
        if ( !result ){
          return callback({ err: err, result: result });
        }
        return callback( err, result.last_id );
      }
    );
  }


  this.insert = function( docs, collection, callback ){ //array of docs to insert
    var seq_ids_to_fetch = 0;
    docs.forEach(function(d){
        if ( d._seqID == "new"  ){
           seq_ids_to_fetch++;
        }
    });

    if ( seq_ids_to_fetch > 0 ) {
      l("Fetching new seq_ids", seq_ids_to_fetch);
      return self._fetch_seq_ids( collection, seq_ids_to_fetch, function(err, next_id){

        if ( err ) return callback(err);

        docs.forEach(function(d){
          if ( d._seqID == "new"  ){
            delete d._seqID;
            d._id = 1 * next_id;
            next_id++; 
          }
        });

        l("Got seqIDs", docs[0]._id);
        l(" inserting docs: ");
        l(docs);
        return self.db.collection(collection).insert(
          docs,
          null,
          callback
        );
      });
    }

    return self.db.collection(collection).insert(
      docs,
      null,
      callback
    );
  };


  this.update = function( crit, newOb, collection, callback ){
    var opts = { safe: true };  
    if (crit.upsert){ opts.upsert = true; delete crit.upsert; }
    if (crit.multi){ opts.multi= true; delete crit.multi; }
    crit = self._resolve_ids(crit);

    var method_name = "update";
    //l("\n*** update crit:", crit);
    self.db.collection(collection)[method_name](crit, newOb, opts, function(err,n,ur){
      if (err){
        l("got err from update\n");
        l(method_name,crit,newOb,collection,err,n,ur);
        l("\n");
      }
      else if (ur.upserted) ur.upserted = {$oid: ur.upserted.toString()};
      //l("update result:", ur);
      return callback(err,ur);
    });
  };

  this.remove = function( crit, collection, callback ){
    crit = self._resolve_ids(crit);
    l("crit:",crit);
    self.db.collection(collection).remove(
      crit,
      {},
      callback
    );
  };

  this.findAndModify = function( params, collection, callback){ //array of docs to insert
    if ( params.sort == undefined ) params.sort={};
    var opts = {};
    if ( true===params.new ){ opts.new = true;  delete params.new; }
    if ( true===params.upsert ){ opts.upsert = true;  delete params.upsert; }
    if ( true===params.remove ){ opts.remove = true;  delete params.remove; }
    if ( undefined!==params.fields ){ opts.fields = params.fields; };
    
    //l("findAndModify opts:", opts);
    //l("findAndModify newobj:", params.newobj);
    self.db.collection(collection).findAndModify(
      params.query,
      params.sort,
      params.newobj,
      opts,
      function( err, result, ur ){
        if ( err ){
          l("got findAndModify error:", err);
        }
        if ( result ){ 
          ur.result = result;
          delete ur.value;
        }
        return callback( err, ur );
      }
    );
  };

  this.lock = function( coll, crit, lock_id, fields, cb ){
    crit.locked = false;
    l("lock crit:", crit);
    self.db.collection(coll).findAndModify(
      crit, 
      { }, 
      { $set: { "locked": lock_id } },
      { fields: fields },
      function( err, found_doc, ur ){
        l("result of mdb lock:",err, found_doc);
        if ( err ) return cb(err);
        return cb( err, found_doc);
      }
    );
  }

  this.unlock = function( coll, crit, lock_id, fields, cb ){
    crit.locked = lock_id;
    self.db.collection(coll).findAndModify(
      crit, 
      { }, 
      { $set: { "locked": false } },
      { fields: fields },
      function( err, found_doc, ur ){
        l("result of mdb UN-lock:",err, found_doc);
        if ( err ) return cb(err);
        return cb( err, found_doc);
      }
    );
  }

  this.count = function( coll, crit, callback ){
    self.db.collection( coll ).count( crit, callback);
  }


  this.find = function (coll,opts,callback){
    var docs = [];
    if (opts.id){ // use an existing cursor
      var ec = self.cursor_ref[ opts.id.toString() ];
      if ( typeof ec == "object" ){
        if ( ec.cursor.isClosed() )
          return callback({ msg: "Cursor no longer active" , opts: opts}, [], 0, 0);
      }
      else return callback({ msg: "invalid cursor id", opts: opts }, [], 0, 0);

      return self._get_batch( ec, callback);
    }
    var expire_cursor=false;
    if ( opts.keep_cursor === true ){
      delete opts.keep_cursor ;
    }
    else{
      expire_cursor=true;
    }


    var method_name = "find";
    var selector = opts.criteria;
    if ( typeof opts.criteria._id === "object" ){
      if ( "string" === typeof opts.criteria._id.$oid ){
        method_name = "findById";
        selector = crit._id.$oid;
      }
    }

    var o = {};
    if ( opts.fields ) o.fields = opts.fields;
    if ( opts.sort ) o.sort = opts.sort;
      
    self.db.collection( coll ).count( selector, function(err,totalDocs){
      if ( totalDocs == 0 ){
        return callback( null, [], 0, 0);
      }
      
      var skinCur = self.db.collection( coll )[method_name]( selector, o );
      self.cursors_created++;

      if ( opts.batch_size ) skinCur.batch_size = opts.batch_size;

      skinCur.docs_this_batch = [];
      skinCur.cursor_pos = 0;
      skinCur.totalDocsThisQuery = 1 * totalDocs;

      if (skinCur.batch_size >= totalDocs)
        return skinCur.toArray( function(err,recs){
          callback(err,recs,0,totalDocs);
        }); //don't need to batchify 


      //l("Need to batcify, batch_size: " + skinCur.batch_size + ", total: " + totalDocs);
      skinCur.cName = self.cursors_created.toString();
      self.cursor_ref[ skinCur.cName ] = skinCur;
      if ( expire_cursor )
        setTimeout( function(){self._clear_cursor(skinCur.cName)}, 25 * 60 * 1000 );

      self._get_batch( skinCur, callback);
    });
  }

  this._clear_cursor = function(cName){
    //l("Checking cursor status " + cName);
    if ( self.cursor_ref[cName] !== undefined ){
      var c = self.cursor_ref[ cName ];
      if ( c.cursor ){
        l("deleting closed cursor: " , cName);
        delete self.cursor_ref[cName];
      }
    }
    else l("found undefined: " + cName);
  }

  this._get_batch = function( sc, callback ){
    
    sc.nextObject( function( err, doc ){
      if ( err ){
        return callback( err, [], sc.cName, sc.totalDocsThisQuery);
      }

      sc.docs_this_batch.push( doc );
      sc.cursor_pos++;
      if ( sc.docs_this_batch.length === sc.batch_size || sc.cursor_pos == sc.totalDocsThisQuery){
        callback( 
          null, 
          [].concat( sc.docs_this_batch ), 
          sc.cName,
          sc.totalDocsThisQuery
        );
        sc.docs_this_batch = []; //clear the current batch 
        return;
      }
      return self._get_batch( sc, callback );
    });
  }



  this.init = function (config, cb){
    try {
    self.db = mongoskin.db( 
      //config.mongodHost +":"+config.mongodListenPort+"/"+config.mongoDBName ,
      "mongodb://"+config.mongodHost +":"+config.mongodListenPort+"/"+config.mongoDBName ,
      {
        auto_reconnect: true,
        w: 1,
        journal: true
      }
    );
    }
    catch(e){
      l(e);
      process.exit();
    }

    return cb();
  };
  return this;
}
