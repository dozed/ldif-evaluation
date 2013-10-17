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
      navigateTo(sourceId - 1);
    } else if(e.keyCode == 39) {
      navigateTo(sourceId + 1);
    }
  });

  $.ajax({
    type: "GET",
    url: "/dbpedia/redirect/" + sourceId,
    success: function(data) {
      if (data.length > 0) {
        var el = '<a target="_blank" href="' + data + '">' + data + '</a>';
        $("#redirect").html(el);
      }
    }
  });

  $.ajax({
    type: "GET",
    url: "/wikipedia/search?query=" + sourceLabel,
    success: function(data) {
      var el = _.chain(data.query.search)
        .sortBy("size")
        .reverse()
        .map(function(r) { return '<li><a target="_blank" href="http://en.wikipedia.org/wiki/' + r.title + '">' + r.title + ' (' + r.size + ')</a></li>'; })
        .value()
        .join("");
      el = "<ul>" + el + "</ul>";
      $("#wikipedia").html(el);
    }
  });

  $.ajax({
    type: "GET",
    url: "/dbpedia/keywordSearch?query=" + sourceLabel,
    success: function(data) {

      var transformResult = function(r) {
        var classes = _.pluck(r.classes, "label").join(", ");
        var cats = _.pluck(r.categories, "label").join(", ");
        return '<li><a href="' + r.uri + '">' + r.label + '</a> classes: ' + classes + ' categories: ' + cats + '</li>';
      }

      console.log(data);
      var el = _.chain(data.results)
        .sortBy("refCount")
        .reverse()
        .map(transformResult)
        .value()
        .join("");
      el = "<ul>" + el + "</ul>";
      $("#dbpedia").html(el);
    }
  });
});