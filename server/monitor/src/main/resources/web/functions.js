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

var QUANTITY_SUFFIX = ["", "K", "M", "B", "T", "e15", "e18", "e21"]
var SIZE_SUFFIX = ["", "K", "M", "G", "T", "P", "E", "Z"]

function toggle(selection) {
  var p = document.getElementById(selection);
  var style = p.className;
  p.className = style == "hide" ? "show" : "hide";
}

function bigNumberForSize(size) {
    if (size === null) 
        size = 0;
    return bigNumber(size, SIZE_SUFFIX, 1024);
}

function bigNumberForQuantity(quantity) {
    if (quantity === null)
        quantity = 0;
    return bigNumber(quantity, QUANTITY_SUFFIX, 1000);
}

function bigNumber(big, suffixes, base) {
    if (big < base) {
        return big + suffixes[0];
    }
    var exp = Math.floor(Math.log(big) / Math.log(base));
    var val = big / Math.pow(base, exp);
    return val.toFixed(2) + suffixes[exp];
}

function timeDuration(time) {
    var ms, sec, min, hr, day, yr;
    ms = sec = min = hr = day = yr = -1;
    
    time = Math.floor(time);
    if (time == 0) {
        return "&mdash;";
    }
    
    ms = time % 1000;
    time = Math.floor(time / 1000);
    if (time == 0) {
        return ms + "ms";
    }
    
    sec = time % 60;
    time = Math.floor(time / 60);
    if (time == 0) {
        return sec + "s" + "&nbsp;" + ms + "ms";
    }
    
    min = time % 60;
    time = Math.floor(time / 60);
    if (time == 0) {
        return min + "m" + "&nbsp;" + sec + "s";
    }
    
    hr = time % 24;
    time = Math.floor(time / 24);
    if (time == 0) {
        return hr + "h" + "&nbsp;" + min + "m";
    }
    
    day = time % 365;
    time = Math.floor(time / 365);
    if (time == 0) {
        return day + "d" + "&nbsp;" + hr + "h";
    }
    
    yr = Math.floor(time);
    return yr + "y" + "&nbsp;" + day + "d";
}

function sortTables(tableID, direction, n) {
  var table, rows, switching, i, x, y, h, shouldSwitch, dir, switchcount = 0, xOne, xTwo, xFinal, yOne, yTwo, yFinal;
  table = document.getElementById(tableID);
  switching = true;
  
  //Set the sorting direction to ascending:
  dir = direction;
  sessionStorage.direction = dir;
  
  rows = table.getElementsByTagName("TR");
  
  var count = 0;
  while (rows[0].getElementsByTagName("TH").length > count) {
    var tmpH = rows[0].getElementsByTagName("TH")[count];
    tmpH.classList.remove("sortable");
    if (rows.length > 2) {
        tmpH.classList.add("sortable");
    }
    $(tmpH.getElementsByTagName("img")).remove();
    count += 1;
  }
  
  if (rows.length <= 2) {
      switching = false;
  } else {
    h = rows[0].getElementsByTagName("TH")[n];
    if (dir == "asc") {
      $(h).append("<img width='10px' height='10px' src='http://localhost:9995/web/up.gif' alt='v' />");
    } else if (dir == "desc") {
      $(h).append("<img width='10px' height='10px' src='http://localhost:9995/web/down.gif' alt='^' />");
    }
  }
  
  /*Make a loop that will continue until
  no switching has been done:*/
  while (switching) {
    //start by saying: no switching is done:
    switching = false;
    rows = table.getElementsByTagName("TR");
    
    /*Loop through all table rows (except the
    first, which contains table headers):*/
    for (i = 1; i < (rows.length - 1); i++) {
      //start by saying there should be no switching:
      shouldSwitch = false;
      /*Get the two elements you want to compare,
      one from current row and one from the next:*/
      x = rows[i].getElementsByTagName("TD")[n].getAttribute("data-value");
      xFinal = (x === "-" || x === "&mdash;" ? null : (Number(x) == x ? Number(x) : x));
      y = rows[i + 1].getElementsByTagName("TD")[n].getAttribute("data-value");
      yFinal = (y === "-" || y === "&mdash;" ? null : (Number(y) == y ? Number(y) : y));
      
      /*check if the two rows should switch place,
      based on the direction, asc or desc:*/
      if (dir == "asc") {
        //if (x.innerText.toLowerCase() > y.innerText.toLowerCase()) {
        if (xFinal > yFinal || yFinal === null) {
          //if so, mark as a switch and break the loop:
          shouldSwitch= true;
          break;
        }
      } else if (dir == "desc") {
        //if (x.innerText.toLowerCase() < y.innerText.toLowerCase()) {
        if (xFinal < yFinal || xFinal === null) {
          //if so, mark as a switch and break the loop:
          shouldSwitch= true;
          break;
        }
      }
    }
    if (shouldSwitch) {
      /*If a switch has been marked, make the switch
      and mark that a switch has been done:*/
      rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
      switching = true;
      //Each time a switch is done, increase this count by 1:
      switchcount ++;
    }
  }
}










