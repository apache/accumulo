/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Server Stats
var statNames = {}, maxStatValues = {}, adjustMax = {}, significance = {};

/**
 * Sets the options for the server visualization
 *
 * @param {string} shape Circle or Square
 * @param {number} size Size of shape
 * @param {string} motion Stat to display motion
 * @param {string} color Stat to display color
 */
function setOptions(shape, size, motion, color) {
  var data = sessionStorage.serverStats === undefined ?
      {} : JSON.parse(sessionStorage.serverStats);
  $('#motion').empty();
  $('#color').empty();

  $.each(data.serverStats, function(key, val) {
    var item = val.description;

    $('#motion').append('<option ' + (motion === val.name ? 'selected' : '') +
        '>' + item + '</option>');
    $('#color').append('<option ' + (color === val.name ? 'selected' : '') +
        '>' + item + '</option>');

  });
  if (speedStatType === undefined) {
    speedStatType = motion !== undefined || motion !== '' ? motion : '';
    speedDisabled = motion !== '' ? false : true;
  }
  if (speedStatType !== motion && motion !== undefined &&
      motion !== 'osload' && motion !== '') {
    speedStatType = motion;
    speedDisabled = false;
  }
  if (colorStatType === undefined) {
    colorStatType = color !== undefined ? color : 'osload';
  }
  if (colorStatType !== color && color !== undefined) {
    colorStatType = color;
  }
}

/**
 * Sets the stats values from the REST call
 */
function setStats() {

  var data = JSON.parse(sessionStorage.serverStats);

  $.each(data.serverStats, function(key, val) {
    statNames[val.name] = val.derived;
    maxStatValues[val.name] = val.max;
    adjustMax[val.name] = val.adjustMax;
    significance[val.name] = val.significance;
  });

  var numNormalStats = 8;
}

// size and spacing variables
var dotSpacing = 10; // spacing between centers of dots (radius)
var dotPadding = 0.5; // dot padding
var minDotRadius = 3; // min dot radius
var maxDotRadius = dotSpacing - dotPadding;

// arrays of information about each dot
var stats = {'servers': []};
var dots = new Array(0); // current sizes and change directions
var mousedDot = -1; // the dot currently under the mouse

var colorPalette = ['#0000CC', '#0014B8', '#0029A3', '#003D8F', '#00527A',
    '#006666', '#007A52', '#008F3D', '#00A329', '#00B814', '#00CC00',
    '#14D100', '#29D600', '#3DDB00', '#52E000', '#66E600', '#7AEB00',
    '#8FF000', '#A3F500', '#B8FA00', '#CCFF00', '#CCFF00', '#CCF200',
    '#CCE600', '#CCD900', '#CCCC00', '#CCBF00', '#CCB200', '#CCA600',
    '#CC9900', '#CC8C00', '#CC8000', '#CC7300', '#CC6600', '#CC5900',
    '#CC4C00', '#CC4000', '#CC3300', '#CC2600', '#CC1A00', '#CC0D00',
    '#CC0000'];

var nullColor = '#F5F8FA';
var deadColor = '#B000CC';

// animation variables
var drawing = false;
var canvas = document.getElementById('visCanvas');
var context = canvas.getContext('2d');

// mouse handling for server information display
document.getElementById('hoverable').addEventListener('mouseover',
    showId, false);
document.getElementById('hoverable').addEventListener('mousemove',
    showId, false);
document.getElementById('hoverable').addEventListener('mouseout',
    hideId, false);
document.getElementById('vishoverinfo').addEventListener('click',
    goToServer, false);
canvas.addEventListener('click', goToServer, false);

// initialize settings based on request parameters
var main = document.getElementById('main');
var speedStatType;
var colorStatType;
var speedDisabled = true;
var useCircles = true;
setShape(document.getElementById('shape'));
setSize(document.getElementById('size'));
setMotion(document.getElementById('motion'));
setColor(document.getElementById('color'));

// xml loading variables
var xmlReturned = true;
// don't bother allowing for IE 5 or 6 since canvas won't work
var xmlhttp = new XMLHttpRequest();
xmlhttp.onreadystatechange = function() {
  handleNewData();
};

self.setInterval('getXML()', 5000);

//self.setInterval('drawDots()',20);

window.requestAnimFrame = (function(callback) {
  return window.requestAnimationFrame ||
  window.webkitRequestAnimationFrame ||
  window.mozRequestAnimationFrame ||
  window.oRequestAnimationFrame ||
  window.msRequestAnimationFrame ||
  function(callback) {
    window.setTimeout(callback, 1000 / 60);
  };
})();

function handleNewData() {
  if (xmlhttp.readyState != 4) {
    return;
  }
  if (xmlhttp.status != 200 || xmlhttp.responseText == null) {
    xmlReturned = true;
    return;
  }
  var newstats = JSON.parse(xmlhttp.responseText);
  for (var i in newstats.servers) {
    for (var s in statNames) {
      if (statNames[s])
        continue;
      newstats.servers[i][s] = Math.max(0, Math.floor(significance[s] *
          newstats.servers[i][s]) / significance[s]);
      if (adjustMax[s])
        maxStatValues[s] = Math.max(newstats.servers[i][s], maxStatValues[s]);
    }
  }

  initDerivedInfo(newstats);
  var oldstats = stats;
  while (drawing) {}
  stats = newstats;
  delete oldstats;
  xmlReturned = true;
}

// set max and average
function setDerivedStats(serverStats) {
  var avgStat = 0;
  var maxStat = 0;
  var maxIndex = 0;
  for (var s in statNames) {
    if (statNames[s])
      continue;
    normStat = serverStats[s] / maxStatValues[s];
    if (normStat > 0)
      avgStat += normStat;
    if (maxStat < normStat) {
      maxStat = normStat;
      maxIndex = s;
    }
  }
  serverStats.allmax = Math.floor(significance.allmax *
      Math.min(1, maxStat)) / significance.allmax;
  serverStats.allavg = Math.floor(significance.allavg *
      avgStat / numNormalStats) / significance.allavg;
  serverStats.maxStat = maxIndex;
}

function initDerivedInfo(newstats) {
  for (var i in newstats.servers) {
    if ('dead' in newstats.servers[i] || 'bad' in newstats.servers[i]) {
      continue;
    }
    if (i >= dots.length) {
      dots.push({'size': maxDotRadius, 'growing': false});
    }
    setDerivedStats(newstats.servers[i]);
  }
}

// construct server info for hover
function getInfo(serverStats) {
  var extra = '<strong>' + serverStats.ip + '</strong>';
  if ('dead' in serverStats || 'bad' in serverStats)
    return extra;
  extra = extra + ' (' + serverStats.hostname + ')';
  var j = 0;
  for (var s in statNames) {
    if (j % 4 == 0)
      extra = extra + '<br>\n';
    extra = extra + '&nbsp;&nbsp;' + s + ': <strong>' +
        serverStats[s] + '</strong>';
    j++;
  }
  extra = extra + ' (' + serverStats.maxStat + ')';
  return extra;
}

// reload xml
function getXML() {
  if (xmlReturned == true) {
    xmlReturned = false;
    xmlhttp.open('GET', jsonurl, true);
    xmlhttp.send();
  }
}

// redraw
function drawDots() {
  requestAnimFrame(drawDots);
  var width = Math.ceil(Math.sqrt(stats.servers.length));
  var height = Math.ceil(stats.servers.length / width);
  var x;
  var y;
  var server;
  var sizeChange;
  drawing = true;
  for (var i in stats.servers) {
    server = stats.servers[i];
    x = i % width;
    y = Math.floor(i / width);
    if ('bad' in server || 'dead' in server) {
      strokeDot(x, y, maxDotRadius - 1, deadColor);
      continue;
    }
    if (speedDisabled || Math.floor(dots[i].size) > maxDotRadius) {
      // check for resize by the user
      dots[i].size = maxDotRadius;
    } else if (server[speedStatType] <= 0) {
      // if not changing size, increase to max radius
      if (dots[i].size < maxDotRadius)
        dots[i].size = dots[i].size + 1;
      if (dots[i].size > maxDotRadius)
        dots[i].size = maxDotRadius;
    } else {
      sizeChange = getStat(i, speedStatType);
      if (dots[i].growing) {
        dots[i].size = dots[i].size + sizeChange;
        if (dots[i].size + sizeChange > maxDotRadius) {
          dots[i].growing = false;
        }
      }
      else {
        dots[i].size = dots[i].size - sizeChange;
        if (dots[i].size - sizeChange < minDotRadius) {
          dots[i].growing = true;
        }
      }
    }
    drawDot(x, y, Math.floor(dots[i].size),
        getColor(getStat(i, colorStatType)));
  }
  /*
   * mousedDot shouldn't be set to an invalid dot,
   * but stats might have changed since then
   */
  if (mousedDot >= 0 && mousedDot < stats.servers.length)
    document.getElementById('vishoverinfo').innerHTML =
        getInfo(stats.servers[mousedDot]);
  drawing = false;
}

// fill in a few grey dots
function drawGrid() {
  context.clearRect(0, 0, canvas.width, canvas.height);
  for (var i = 0, k = 0; i < canvas.width; i += dotSpacing * 2, k++) {
    for (var j = 0, l = 0; j < canvas.height; j += dotSpacing * 2, l++) {
      drawDot(k, l, maxDotRadius, nullColor);
    }
  }
}

// fill a dot specified by indices into dot grid
function drawDot(i, j, r, c) {
  var x = i * dotSpacing * 2 + dotSpacing;
  var y = j * dotSpacing * 2 + dotSpacing;
  context.clearRect(x - dotSpacing, y - dotSpacing, dotSpacing * 2,
      dotSpacing * 2);
  if (useCircles)
    fillCircle(x, y, r - dotPadding, c);
  else
    fillSquare(x - r, y - r, (r - dotPadding) * 2, c);
}

// stroke a dot specified by indices into dot grid
function strokeDot(i, j, r, c) {
  var x = i * dotSpacing * 2 + dotSpacing;
  var y = j * dotSpacing * 2 + dotSpacing;
  context.clearRect(x - dotSpacing, y - dotSpacing, dotSpacing * 2,
      dotSpacing * 2);
  if (useCircles)
    strokeCircle(x, y, r - dotPadding, c);
  else
    strokeSquare(x - r, y - r, (r - dotPadding) * 2, c);
}

function getStat(dotIndex, statIndex) {
  return Math.min(1, stats.servers[dotIndex][statIndex] /
      maxStatValues[statIndex]);
}

// translate color between 0 and maxObservedColor into an html color code
function getColor(normColor) {
  return colorPalette[Math.round((colorPalette.length - 1) * normColor)];
}

function strokeCircle(x, y, r, c) {
  context.strokeStyle = c;
  context.lineWidth = 2;
  context.beginPath();
  context.arc(x, y, r, 0, Math.PI * 2, true);
  context.closePath();
  context.stroke();
}

function fillCircle(x, y, r, c) {
  context.fillStyle = c;
  context.beginPath();
  context.arc(x, y, r, 0, Math.PI * 2, true);
  context.closePath();
  context.fill();
}

function strokeSquare(x, y, d, c) {
  context.strokeStyle = c;
  context.lineWidth = 2;
  context.strokeRect(x, y, d, d);
}

function fillSquare(x, y, d, c) {
  context.fillStyle = c;
  context.fillRect(x, y, d, d);
}

// callback for shape selection
function setShape(obj) {
  switch (obj.selectedIndex) {
    case 0:
      useCircles = true;
      break;
    case 1:
      useCircles = false;
      break;
    default:
      useCircles = true;
  }
  drawGrid();
  setState();
}

// callback for size selection
function setSize(obj) {
  switch (obj.selectedIndex) {
    case 0:
      dotSpacing = 5;
      minDotRadius = 1;
      break;
    case 1:
      dotSpacing = 10;
      minDotRadius = 3;
      break;
    case 2:
      dotSpacing = 20;
      minDotRadius = 5;
      break;
    case 3:
      dotSpacing = 40;
      minDotRadius = 7;
      break;
    default:
      dotSpacing = 10;
      minDotRadius = 3;
  }
  maxDotRadius = dotSpacing - dotPadding;
  drawGrid();
  setState();
}

// callback for motion selection
function setMotion(obj) {
  if (obj.selectedIndex <= 0) {
    speedDisabled = true;
    setState();
    return;
  }
  var i = 0;
  for (var s in statNames) {
    if (i == obj.selectedIndex) {
      speedStatType = s;
      break;
    }
    i++;
  }
  speedDisabled = false;
  setState();
}

// callback for color selection
function setColor(obj) {
  var i = 0;
  for (var s in statNames) {
    if (i == obj.selectedIndex) {
      colorStatType = s;
      break;
    }
    i++;
  }
  setState();
}

// hide server info on mouseout
function hideId(e) {
  document.getElementById('vishoverinfo').style.visibility = 'hidden';
}

// display server info on mouseover
function showId(e) {
  var x;
  var y;
  if (e.pageX || e.pageY) {
    x = e.pageX + main.scrollLeft;
    y = e.pageY + main.scrollTop;
  }
  else {
    // clientX and clientY unimplemented
    return;
  }
  var relx = x - canvas.offsetLeft - main.offsetLeft;
  var rely = y - canvas.offsetTop - main.offsetTop;
  var width = Math.ceil(Math.sqrt(stats.servers.length));
  gotDot = Math.floor(relx / (dotSpacing * 2)) + width *
      Math.floor(rely / (dotSpacing * 2));
  mousedDot = -1;
  if (relx < (width * dotSpacing * 2) && gotDot >= 0 &&
      gotDot < stats.servers.length) {
    mousedDot = gotDot;
    document.getElementById('vishoverinfo').style.left = relx +
        canvas.offsetLeft;
    document.getElementById('vishoverinfo').style.top = Math.max(0,
        rely + canvas.offsetTop - 70);
    document.getElementById('vishoverinfo').style.visibility = 'visible';
  }
  else {
    document.getElementById('vishoverinfo').style.visibility = 'hidden';
  }
}

function setState() {
  var url = visurl + '?shape=' + (useCircles ? 'circles' : 'squares') +
      '&size=' + (dotSpacing * 2) + (speedDisabled ? '' :
      '&motion=' + speedStatType) +
      '&color=' + colorStatType;
  window.history.replaceState(window.history.state, 'Server Activity', url);

  setOptions(useCircles ? 'circles' : 'squares', dotSpacing * 2,
      speedDisabled ? '' : speedStatType, colorStatType);
}

// go to server page on click
function goToServer(e) {
  /*
   * mousedDot shouldn't be set to an invalid dot,
   * but stats might have changed since then
   */
  if (mousedDot >= 0 && mousedDot < stats.servers.length)
    window.location = serverurl + stats.servers[mousedDot].ip;
}

/*window.onload = function() {
  drawGrid();
  drawDots();
  getXML();
}*/
