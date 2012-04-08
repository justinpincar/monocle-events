var express = require('express');
var mongodb = require('mongodb');
var ObjectID = require('mongodb').ObjectID;

var server = new mongodb.Server("staff.mongohq.com", 10085, {auto_reconnect: true});
var client = new mongodb.Db('monocle-production', server, {strict: false});
client.open(function (error, client) {
	if (error) throw error;

        client.authenticate('monocle', 'monocle_mongo', function(error, data) {
		if (error) throw error;

		mongoReady(client);
	    });
    });

function mongoReady(client) {
    var app = express.createServer();
    app.use(express.bodyParser());

    app.get('/', function(req, res){
	    var accountId = req.param("accountId", null);
	    var sessionId = req.param("sessionId", null);
	    var eventId = req.param("eventId", null);

	    if (accountId == null || accountId == "undefined" || sessionId == null) {
		return;
	    }

	    var href = req.param("href", null);
	    var now = new Date();

	    var analyticObjectData = {};
	    if (eventId != null) {
		analyticObjectData.e = new ObjectID(eventId);
	    }
	    analyticObjectData.t = 0;
	    analyticObjectData.h = href;
	    var sessionObjectId = new ObjectID(sessionId);
	    var analyticObject = {s: sessionObjectId, d: analyticObjectData, ts: now};

	    client.collection("analytics_" + accountId, function(error, collection) {
		    if (error) throw error;

		    collection.insert(analyticObject, {safe: false});
		});

	    // cometClient.send(accountId, event.toMap());

	    var identifyId = req.param("identifyId", null);
	    var identifyEmail = req.param("identifyEmail", null);
	    var identifyName = req.param("identifyName", null);

	    var ip = req.header("X-Forwarded-For", null);
	    if(ip == null) {
		ip = req.connection.remoteAddress ? req.connection.remoteAddress : req.remoteAddress;
	    }
	    else {
		var ips = ip.split(",");
		if (ips.length > 0) {
		    ip = ips[ips.length - 1];
		}
	    }

	    var sessionQuery = {"_id": sessionObjectId};
	    var session = {};
	    var set = {"u_at": now};

	    if (identifyId != null) {
		set.i = identifyId;
	    }
	    if (identifyEmail != null) {
		set.e = identifyEmail;
	    }
	    if (identifyName != null) {
		set.d = identifyName;
	    }

	    session.$set = set;

	    if (ip != null) {
		var ipSet = {ips: ip};
		session.$addToSet = ipSet;
	    }

	    client.collection("sessions_" + accountId, function(error, collection) {
		    if (error) throw error;

		    collection.update(sessionQuery, session, {safe: true, upsert: true, multi: false}, function(error) {
			    if (error) throw error;

			    var sessionQueryCreatedAt = {"_id": sessionObjectId, "c_at": null};
			    var sessionUpdate = {"$set": {"c_at": now}};

			    collection.update(sessionQueryCreatedAt, sessionUpdate, {safe: true, upsert: false, multi: false}, function(error) {
				    if (error) throw error;
				});
			});
		});

	    res.contentType("text/javascript");
	    res.send(200);
	});

    app.listen(80);
}
