
require("cloud/app.js");
var request = require('request');
var FeedParser = require('feedparser');
var async = require('async');
var _ = require('underscore');
var rss36Kr = 'http://www.36kr.com/feed';


var rowCount = 0;
var qry = new AV.Query('Product');
qry.count({
    success: function (count) {
        console.log("product rows number: %j", count);
        rowCount = count;
    }
});