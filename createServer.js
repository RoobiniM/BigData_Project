var
_http = require('http'),
_fs = require('fs'),
_url = require('url'),
_path = require('path'),
_assert = require('assert'),
_express = require('express'),
_bodyParser = require('body-parser'),
_cassandra = require('cassandra-driver');

var _client = new _cassandra.Client({ contactPoints: ['199.60.17.136'], keyspace: 'rmathiya'});
var _serverDomainName = 'gateway.sfucloud.ca';
var _app = _express();
const _PORT = 8080; 
var _dirName = '/home/rmathiya/nodejs/scripts';
var _userId = '';
var _result = '';

_app.use(_bodyParser.json()); // support json encoded bodies

_app.get('/', function(req, res) {	
	res.sendFile(_dirName + '/index.html');	
});

_app.get('/login', function(req, res) {	
    if (_userId !== '')
	{
		const query = 'SELECT * FROM test where id=' + _userId;
		_client.execute(query, function(err, result) {
		_assert.ifError(err);
		_result = result.rows[0].data;
		console.log('got the result: ' + _result);});		
		res.send('got the result: ' + _result);	
	}
	else
		res.send('Invalid User Id');
});

_app.post('/', function(req, res) {	
	_userId = req.body.u;
	console.log("userId ---------->", _userId);
	res.redirect('/login');	
});

_app.listen(_PORT, _serverDomainName, function(){
	console.log("Server listening on: http://gateway.sfucloud.ca:%s", _PORT);
});