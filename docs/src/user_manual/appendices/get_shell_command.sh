#!/bin/bash

echo "help $1quit" | ../../../../bin/accumulo shell -u foo -p foo --fake 2>&1 >/dev/null | sed -E "s/  +/  /g" | perl -ne 'use Text::Wrap; $Text::Wrap::columns=82; $Text::Wrap::break="[ ]"; print wrap("","              ",$_);' | gawk '{print "   ",$0}'
