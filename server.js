var PORT=9090;

var express=require('express');
var app=express();
var cors=require('cors');
var http=require('http');
var server= http.createServer(app).listen(process.env.PORT||PORT); //AWS port is selected by process env PORT
var io= require('socket.io')(server);
var twitter=require('twitter');
var env= require('dotenv').config();
var elasticsearch=require('elasticsearch');
var awsEs=require('http-aws-es');
var globalSocket;

//Configuring the twitter credentials(Loading environment variables from .env file)
var twitterCredentials =new twitter({
consumer_key:process.env.consumer_key,
consumer_secret:process.env.consumer_secret,
access_token_key:process.env.access_token_key,
access_token_secret:process.env.access_token_secret
});

//Configuring the AWS Elastic Search Credentials(Loading environment variables from .env file)
var es=new elasticsearch.Client({
  hosts: 'https://search-tweetsdata-7h6cbtt4v6batg45hc4dkiyua4.us-east-2.es.amazonaws.com',
  connectionClass: awsEs,
  amazonES: {
    region: 'us-east-2',
    accessKey: process.env.aws_access_key_id,
    secretKey: process.env.aws_secret_access_key
}
});

var stream=null;

app.use(express.static("./public"));

var item="Donald Trump";

app.get("/:item",function(req,res){
console.log(req.params.item);
item=req.params.item;
 es.cluster.health({},function(err,resp,status) {
  console.log("-- Client Health --",resp);
});


if(item===null||item==="")
{
	item="Donald Trump";
	console.log("Null Check");
}
});

function emitTweets(latlong)
{
  console.log("Emitting Tweets");
  console.log(`The latitude and longitude are :${latlong}`)
  globalSocket.emit("tweetStream", latlong);
}

setInterval(function()
  {
  es.indices.delete({index: 'tweets'},function(err,res,status) {
  console.log("delete",res);
  });
  }
  ,10000);

//Function to save tweets in Elastic Search
function saveInEs(tweet,latlong)
{
  console.log("Saving in elasticsearch");
  var id=tweet.id_str;
  es.index({
    index: 'tweets',
    type: 'tweet',
    id: id,
    body: tweet
  }, function (err, resp) {
    console.info(err);
    console.info(resp);
    if(!err){
      emitTweets(latlong);
    }
  });
}

//Websockets to handle twitter streaming
io.sockets.on('connection', function (socket) {
  globalSocket=socket;
  socket.on("start-streaming", function(item) {
  	console.log("Streaming started");
    console.log(`The item value is:-> ${item}`)
  	if(stream === null)
  	{
      console.log("Stream is null");
      twitterCredentials.stream('statuses/filter', {track:item}, function(stream) {
          stream.on('data', function(tweet) {
            console.log(`item is ${item}`);
          	console.log("Tweets started");
          	console.log(JSON.stringify(tweet));
               if(tweet.place)
                {
                  if(tweet.place.bounding_box)
                  {
                  console.log("Bounding box entered");
                  if(tweet.place.bounding_box)
                   {
                  if(tweet.place.bounding_box.type==='Polygon'){
                  var crd=tweet.place.bounding_box.coordinates[0][0];
                  var latlong = {"latitude": crd[0],"longitude": crd[1],"title":tweet.place.full_name};
                  saveInEs(tweet,latlong);
                }
              }
              }
              }
          });

            stream.on('error', function(error) {
                throw error;
              });

              stream.on('limit', function(msg) {
               console.log(msg);
              });

              stream.on('warning', function(warning) {
               console.log(msg);
              });

              stream.on('disconnect', function(msg) {
                console.log(msg);
              });
      });
  	};
  });

  socket.on('error',function(error)
    {
      throw error;
      io.connect(server, {
      'force new connection': true
      });

    });

    socket.emit("connected");
});


console.log("Listening on port: "+PORT);
