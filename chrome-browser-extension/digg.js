var render = function(data) {
    var html = "<li>";
    html += "<img src=\""+data.item.thumbnail.src+ "\">";
    html += "<p>";
    html += "<a target=\"_new\" href=\""+data.item.href+"\">"+data.item.title+"</a>";
    html += "</p>";
    html += "<div class=\"meta\">";
    html += "<span class=\"author\">submitted by <a href=\"http://digg.com/"+data.user.name+"\">"+data.user.name+"</a></span>";
    html += "</div>";
    html += "</li>";
    return html;
}

var setup = function() {
  var evt =  new EventSource('http://services.digg.com/2.0/stream?types=submission&format=event-stream');
  evt.onmessage = function(e) {
      var data = JSON.parse(e.data);
      $("#events").prepend(render(data));
      $("li:gt(4)").remove()

  }
}

setup();

