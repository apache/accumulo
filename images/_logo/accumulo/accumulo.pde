/**
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

/** Draws an accumulo logo */

int celloffset = 8; // cell spacing
int cellsize  = 50; // cell size

int u   = 6; // horizontal thickness
int v   = 6; // vertical thickness
int cr  = 8; // corner radius

int ft  = 2;  // frame thickness
int fcr = 4;  // frame corner radius

color bg  = color(255, 255, 255); // background color, white
color mid = color(192, 192, 192); // frame color, grey.
color fg  = color(  0, 0, 0); // foreground color, black.

// width and height shouldn't change.
int width  = 11; // width of frame in cells
int height = 3; // height of frame in cells

void setup() {
  size(550, 150);
  smooth();
  noLoop();
  ellipseMode(CORNERS);
  strokeCap(ROUND);
  noFill();
}

void draw() {
  background(bg);
  frame();
  accumulo();
}

/** Draw the frame around the logo */
void frame() {
  stroke(mid); // stroke color

  int t = 0, b = 0, l = 0, r = 0;
  int w = 2; // line weight in pixels

  for (int x=0; x < width; x++) {
    for (int y=0; y < height; y++) {
      l = (x > 0) ? ft : 0;
      r = (x < width-1) ? ft: 0;
      t = (y > 0) ? ft : 0;
      b = (y < height-1) ? ft: 0;

      t = (y > 0 && y < (height-1) && x > 0 && x < width-1) ? 0: t;
      b = (y > 0 && y < (height-1) && x > 0 && x < width-1) ? 0: b;
      l = (y > 0 && y < (height-1) && x > 0 && x < width-1) ? 0: l;
      r = (y > 0 && y < (height-1) && x > 0 && x < width-1) ? 0: r;

      shape(x, y, t, b, l, r, 0, 0, 0, 0, fcr);
    }
  }
} 

/** Draw the letters in the logo */
void accumulo() {
  stroke(fg); // stroke color

  int pos = 0; // current position

  shape(++pos, 1, u, u, 0, v, v, u, 0, 0, cr);   // a
  
  //shape(++pos, 1, u, 0, v, v, 0, u, 0, 0, cr);   // a
  shape(++pos, 1, u, u, v, 0, 0, 0, 0, 0, cr);   // c
  shape(++pos, 1, u, u, v, 0, 0, 0, 0, 0, cr);   // c
  shape(++pos, 1, 0, u, v, v, 0, 0, 0, 0, cr);   // u
  shape(++pos, 1, u, 0, v, v, 0, 0, u + 2, 0, cr);   // n, biased right
  shape(++pos, 1, u, 0, v, v, 0, 0, 0, 0 - u - 2, cr);  // n, biased left
  shape(++pos, 1, 0, u, v, v, 0, 0, 0, 0, cr);   // u
  shape(++pos, 1, 0, u, v, 0, 0, 0, 0, 0, cr);   // l
  shape(++pos, 1, u, u, v, v, 0, 0, 0, 0, cr);   // o
}

/** Render a shape at the specified position, using the line widths provided.
 *  Think a lame version of a multi-segment display.
 *
 *  If the width of a segment is set to zero, the line won't be rendered.
 *
 * @param x:  x position
 * @param y:  y position
 * @param t:  top line width
 * @param b:  bottom line width
 * @param l:  left line width
 * @param r:  right line width
 * @param lm: left mid-line width (from bottom to middle on the left side)
 * @param c:  center line width
 * @param br: horizontal bias for right side
 * @param bl: horizontal bias for left side
 */

void shape(int x, int y, int t, int b, int l, int r, int lm, int c, int br, int bl, int cr) {
  int x1 = x*cellsize;
  int y1 = y*cellsize;

  x1 += celloffset;
  y1 += celloffset;

  int def = cellsize - (celloffset*2);

  int x2 = x1+def;
  int y2 = y1+def;

  // top
  if (t > 0) {
    int cbl = 0;
    if (l > 0 || lm > 0) cbl = cr;

    int cbr = 0;
    if (r > 0) cbr = 0 - cr;

    strokeWeight(t);
    line(x1+bl+cbl, y1, x2+br+cbr, y1); // top

    if (cr > 0) {
      // top left arc
      if (l > 0 || lm > 0) {
        arc(x1+bl, y1, x1+2*cr+bl, y1+2*cr, PI, PI+PI/2);
      }

      // top right arc.
      if (r > 0) {
        arc(x2-2*cr+br, y1, x2+br, y1+2*cr, PI+PI/2, TWO_PI);
      }
    }
  }

  // right
  if (r > 0) {
    int cbt = 0;
    if (t > 0) cbt = cr;

    int cbb = 0;
    if (b > 0) cbb = 0 - cr; 

    strokeWeight(r);
    line(x2+br, y1+cbt, x2+br, y2+cbb); // right
  }

  // bottom
  if (b > 0) {
    int cbl = 0;
    if (l > 0 || lm > 0) cbl = cr;

    int cbr = 0;
    if (r > 0) cbr = 0 - cr;

    strokeWeight(b);
    //line(x2+cbl,y2,x1+cbr,y2); // bottom
    line(x1+cbl, y2, x2+cbr, y2); // bottom

    if (cr > 0) {
      // bottom left arc
      if (l > 0 || lm > 0) {
        arc(x1, y2-2*cr, x1+2*cr, y2, PI/2, PI);
      }

      // bottom right arc.
      if (r > 0) {
        arc(x2-2*cr, y2-2*cr, x2, y2, 0, PI/2);
      }
    }
  }

  // left
  if (l > 0) {
    int cbt = 0;
    if (t > 0) cbt = cr;

    int cbb = 0;
    if (b > 0) cbb = 0 - cr; 


    strokeWeight(l);
    line(x1+bl, y1+cbt, x1+bl, y2+cbb); // left
  }

  // mid-left, e.g for the left side of the a
  if (lm > 0) {
    int cbt = 0;
    if (t > 0) cbt = cr;
    
    int cbb = 0;
    if (b > 0) cbb = 0 - cr;  

    strokeWeight(lm);
    int delta = (y1-y2)/2; 
    line (x1, y1-delta+cbt, x1, y2+cbb);
  }

  // center, e.g for the horizontal member of the a
  if (c > 0) {
    int cbl = 0;
    if (lm > 0) {
      cbl = cr;
    }
    
    strokeWeight(c);
    int delta = (y1-y2)/2; 
    line(x1+cbl, y1-delta, x2, y1-delta);
    
    if (lm > 0) {
      arc(x1+bl, y1-delta, x1+2*cr+bl, y1-delta+2*cr, PI, PI+PI/2);
    }
  }
}

void mousePressed() 
{
  save("accumulo-logo.png");
}
