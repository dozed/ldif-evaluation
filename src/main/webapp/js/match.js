// val sourceId = 0;
// val sourceLabel = "";

function urlParam(name){
  var results = new RegExp('[\\?&]' + name + '=([^&#]*)').exec(window.location.href);
  if (results==null){
   return null;
  } else {
   return results[1] || 0;
  }
}

function navigateTo(id) {
  var url = "/match/" + id;

  var t = urlParam("threshold");
  var skip = urlParam("skipExact");

  if (t != null || skip != null) url += "?";
  if (t != null) url += "threshold=" + t;
  if (skip != null) url += "&skipExact=" + skip;

  location.href = url;
}

function acceptMatch(from, to) {
  $.ajax({
    type: "POST",
    url: "/match/" + from + "/" + to,
    success: function() {
        navigateTo(sourceId + 1);
    }
  });
}

function removeMatch(from, to) {
  $.ajax({
    type: "DELETE",
    url: "/match/" + from + "/" + to,
    success: function() {
        location.reload();
    }
  });
}

$(function() {
  $("body").keydown(function(e) {
    if(e.keyCode == 37) {
      navigateTo(sourceId - 1;
    } else if(e.keyCode == 39) {
      navigateTo(sourceId + 1);
    }
  });

  $.ajax({
    type: "GET",
    url: "/dbpedia/redirect/" + sourceId,
    success: function(data) {
      $("#redirect").html(data);
    }
  });

  $.ajax({
    type: "GET",
    url: "/wikipedia/search?query=" + sourceLabel,
    success: function(data) {
      console.log(data);
    }
  })
});