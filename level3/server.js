var url = require('url');
var server = require('node-router').getServer();
var fs = require('fs');
var path = require('path');
var exec = require('child_process').exec;
var files = []
var index = {};
var indexing = {};
var data_path = "test/data/input"

var cache = {}
var query_data = function(k, resp) {
    if(k in cache) {
        resp.simpleJson(200, cache[k]);
        return;
    }

    var child = exec("cd " + data_path + "; grep -rno " + k + " | cut -d: -f1,2 | sort | uniq",
            function(error, stdout, stderr) {
                if(error || stdout === undefined) {
                    resp.simpleJson(200, {success: false, results: []});
                    return;
                }
        var dataArr = stdout.split('\n');
        dataArr.splice(dataArr.length - 1, 1);
        cache[k] = {
            success: dataArr.length != 0,
            results: dataArr
        };
        resp.simpleJson(200, cache[k]);
    });
}

var build_results = function(k) {
    var locations = index[k];
    var i = 0;
    var result = [];
    for(; i < locations.length; ++i) {
        result[result.length] = files[locations[i][0]] + ":" + locations[i][1];
    }
    return result;
}

var insert = function(data, fp) {
    var file_index = files.length;
    files[file_index] = fp;
    var lines = data.split("\n");
    var i = 0;
    for(; i < lines.length; ++i) {
        var j = 0;
        var words = lines[i].split(/\s/);
        for(; j < words.length; ++j) {
            if(words[j] in index) {
                index[words[j]][index[words[j]].length] = [file_index, i]
            }
        }
    }
}

var read = function(dirPath) {
    indexing[dirPath] = true;
    var walk = require('walk'),
        options, walker;

    walker = walk.walk(dirPath, { followLinks: false });
    walker.on("file", function(root, fileStats, next) {
        var fp = path.join(root, fileStats.name);
        fs.readFile(fp, 'utf8', function (err, data) {
            if(err) console.log(fp + "was not read: " + err);
            var insname = fp.substring(dirPath.length);
            if(insname[0] == '/') insname = insname.substring(1)
            insert(data, insname);
        });
        next();
    });
    walker.on("end", function() {
        console.log("Finished indexing: " + dirPath);
        delete indexing[dirPath];
    });
}



// Configure our HTTP server to respond with Hello World the root request
server.get("/healthcheck", function (request, response) {
  response.simpleJson(200,  {success: true});
});

server.get("/index", function(request, response) {
    var args = url.parse(request.url, true);
        response.simpleJson(200, {success: true});
        data_path = args.query.path;
    return;
    if('path' in args.query) {
        response.simpleJson(200, {success: true});
        setTimeout(function() {
            read(args.query.path);
        }, 200);
    }
    else {
        response.simpleText(400, "Invalid request");
    }
});

server.get("/isIndexed", function(request, response) {
    response.simpleJson(200, {success: Object.keys(indexing).length == 0});
});

server.get("/", function(request, response) {
    console.log(request.url);
    var args = url.parse(request.url, true);
    query_data(args.query.q, response);
    return;
    if('q' in args.query) {
        if(args.query.q in index) {
            response.simpleJson(200, {
                success: true,
                results: build_results(args.query.q)
            });
        } else {
            response.simpleJson(200, {
                success: false
            });
        }
    }
    else {
        response.simpleText(400, "Invalid request");
    }
});

/*
fs.readFile('words.dat', 'utf-8', function(err, data) {
    var words = data.split(/\s/);
    var i = 0;
    for(;i < words.length; ++i) {
        index[words[i]] = [];
    }
});*/

// Listen on port 8080 on localhost
server.listen(9090, "localhost");

