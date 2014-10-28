require("cloud/app.js");
var request = require('request');
var FeedParser = require('feedparser');
var async = require('async');
var _ = require('underscore');
var rss36Kr = 'http://www.36kr.com/feed';


var avTechhackAppId = 'xv1cgfapsn90hyy2a42i9q6jg7phbfmdpt1404li6n93tt2r';
var avTechhackKey = '70sp4h8prccxzyfp56vwm9ksczji36bsrjvtwzvrzegfza67';
var avTechhack;

function readRSS(callback) {

    var req = request(rss36Kr);
    var feedParser = new FeedParser();
    var newsList = [];

    req.on('error', function (error) {
        // handle any request errors
    });
    req.on('response', function (res) {
        var stream = this;

        if (res.statusCode != 200) return this.emit('error', new Error('Bad status code'));

        stream.pipe(feedParser);
    });

    feedParser.on('error', function (error) {
        // always handle errors
    });

    // every item is fucking 'readable', need to wait util stream ended
    feedParser.on('readable', function () {
        // This is where the action is!
        var stream = this
            , meta = this.meta // **NOTE** the "meta" is always available in the context of the feedparser instance
            , item;

        while (item = stream.read()) {
            newsList.push(item);
        }
    });

    feedParser.on('end', function () {
        console.log('total newsList got: %j', newsList.length);
        callback(newsList);
    });
//  items needed: newTitle, newsSrc, newsLink, relatedProducts (array of pointers)
}


var ContextLoader = function (appId, appKey) {
    var runInThis = function () {
        eval(this.script);
        this.AV.initialize(appId, appKey);
    };
    this.loadContext = function (callback) {
        request({url: 'https://leancloud.cn/scripts/lib/av-0.4.4.min.js'},
            function (error, response, body) {
                this.script = body;
                runInThis.call(this);
                callback(this.AV);
            })
    }
};

var initializeAV =function(appId,appKey,callback){
    new ContextLoader(appId, appKey).loadContext(function (context) {
        callback(null,context);
    })
};

function getAllRows(tableName, colName, unique, callback) {
    var rows = [];
    var rowCount = 0;

    async.series([
            function (callback) {
                initializeAV(avTechhackAppId, avTechhackKey, function(a, context) {
                    avTechhack = context;
                    callback();
                });
            },
            // get row count
            function (callback) {
                console.log("get row count");

                var qry = new avTechhack.Query('Product');
                qry.count({
                    success: function (count) {
                        console.log("product rows number: %j", count);
                        rowCount = count;
                        callback();
                    }
                });
            },
            // get all rows and put them into productNames
            function (callback) {
                console.log("get all rows");
                var step;
                var calls = [];
                step = 500;

                var starts = [];
                for (var i = 0; i <= rowCount; i += step) {
                    starts.push(i);
                }

                starts.forEach(function (s) {
                    var qry = new avTechhack.Query(tableName);
                    if (colName != null) {
                        qry.select(colName);
                    }

                    qry.skip(s);
                    qry.limit(step);

                    calls.push(function (cb) {
                        qry.find({
                            success: function (results) {
                                console.log("batch searched: %j", results.length);
                                results.forEach(function (p) {
                                    rows.push(p);
                                });
                                cb();
                            },
                            error: function (err) {
                                console.log("err: %j", err);
                                cb();
                            }
                        })
                    })
                });

                async.parallel(calls, function (err, result) {
                    if (err) {
                        console.log("err in parallel func: %j", err);
                        return err;
                    } else {
                        console.log("result in parallel func: %j", result);
                    }
                    callback();
                });
            },
            // dedup if necessary
            function (callback) {
                if (colName != null && unique) {
                    var m = _.groupBy(rows, function (row) {
                        return row.get(colName);
                    });

                    var tempRows = [];

                    Object.keys(m).forEach(function (key) {
                        console.log("key: %j, rows[key]: %j", key, m[key]);
                        tempRows.push(m[key][0]);
                    });

                    rows = tempRows;
                }
                callback()
            }
        ],
        // final callback
        function (err, result) {
            callback(rows);
        });
}

function belongsTo(title, products) {
    var candidates = [];
    products.forEach(function (p) {
        if (title.indexOf(p.get('name')) != -1) {
            candidates.push(p);
        }
    });
    return candidates;
}

function newNewsObj(rawNews, products, source) {
    var News = avTechhack.Object.extend('News');
    var news = new News();

    news.set("newsLink", rawNews.link);
    news.set("newsSrc", rawNews.src);
    news.set("newsTitle", rawNews.title);
    news.set("newsSrc", source);

    var relatedProducts = belongsTo(rawNews.title, products);
    if (relatedProducts.length > 0) {
        var relation = news.relation("relatedProducts");
        relation.add(relatedProducts);
    }

    return news;
}

function avosNewsSave(newsList, products, historyNews, callback) {
    // if news exist in history, it's quite old
    var tempList = [];

    console.log("news from RSS size: %j", newsList);

    newsList.forEach(function (n) {
        var isHit = false;
        historyNews.forEach(function (hn) {
            var newsLink = n.link;
            var historyLink = hn.get("newsLink");

            if (newsLink.indexOf(historyLink) != -1 ||
                historyLink.indexOf(newsLink) != -1) {
                isHit = true
            }
        });

        if (!isHit) {
            tempList.push(n);
        }
    });
    newsList = tempList;

    var avNewsList = [];
    newsList.forEach(function (rawNews) {
        avNewsList.push(newNewsObj(rawNews, products, rss36Kr));
    });

    var promises = [];
    _.each(avNewsList, function (avNews) {
        console.log("news title: %j", avNews.get('newsTitle'));
        promises.push(avNews.save());
    });

    avTechhack.Promise.when(promises).then(
        function (results) {
            console.log("news saved to AV size: %j", newsList);
            callback(results);
        },
        function (error) {
            console.log("avosNewsSave error: %j", error);
            callback(null);
        }
    );
}

function main() {

    console.log("main");

    var newsList = []; // from rss
    var products = []; // from avos
    var historyNews = []; // from avos

    async.series([
            function (callback) {
                readRSS(function (ns) {
                    console.log("got RSS %j", ns.length);
                    newsList = ns;
                    callback();
                })
            },
            function (callback) {
                // get all product names from table Product
                getAllRows('Product', 'name', true, function (pns) {
                    console.log("got Products %j", pns.length);
                    products = pns;
                    callback();
                })
            },
            function (callback) {
                // get all newsList links from table News36Kr, then dedup
                getAllRows('News', 'newsLink', true, function (hns) {
                    console.log("got News Links %j", hns.length);
                    historyNews = hns;
                    callback();
                })
            },
            function (callback) {
                avosNewsSave(newsList, products, historyNews, function (results) {
                    if (results != null) {
                        console.log("new news saved");
                    } else {
                        console.log("nothing saved");
                    }
                    callback();
                });
            }
        ],
        // final callback
        function () {
            console.log("batch fin: " + new Date());
        });
}
// Use AV.Cloud.define to define as many cloud functions as you want.
// For example:
AV.Cloud.define("hello", function (request, response) {
    main();
    response.success("batch fin: " + new Date());
});