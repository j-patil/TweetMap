 var map;
 var markers=[];
 var socket =io();
  socket.on("connected", function() {
      console.log("Connected to the server socket");
    });

 socket.on("disconnected",function()
  {
    console.log("Disconnected from the server");
  });

 socket.on("tweetStream",function(tweet)
  {
    console.log("Connected to the twitter stream");
    var location=new google.maps.LatLng(tweet.longitude,tweet.latitude);
    var title=tweet.title;
    addMarker(location,title);
  });

  function initMap() {
          map = new google.maps.Map(document.getElementById('map'), {
          center: {lat: 51.5074, lng: 0.1278},
          zoom: 2
        });
      };

  function addMarker(location,title) {
        var marker = new google.maps.Marker({
          position: location,
          map: map,
          title:title
        });
        markers.push(marker);
      };

  function setMapOnAll(map) {
        for (var i = 0; i < markers.length; i++) {
          markers[i].setMap(map);
        }
      };

  function deleteMarkers() {
        setMapOnAll(null);
        markers = [];
      };  
  
  function selectItem(value)
  {
    if(value==null||value=="")
    {
      alert("Select some item to begin with");
    }
    else{  
    alert("Tweets for "+value.toUpperCase()+" will start now");
    deleteMarkers();
    socket.emit("start-streaming",value);
    $ajax(
    {
      url:"/"+value,
      type:"GET"
    })
    }
  };  