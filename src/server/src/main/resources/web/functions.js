function toggle(selection) {
  var p = document.getElementById(selection);
  var style = p.className;
  p.className = style == "hide" ? "show" : "hide";
}
