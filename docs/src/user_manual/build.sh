#! /bin/bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
cd $bin

test -x /usr/bin/pdflatex || exit 0

if [ -f ../../accumulo_user_manual.pdf ]
then
  if [ `find . -name '*.tex' -newer ../../accumulo_user_manual.pdf | wc -l` -eq 0 ] 
  then
   exit 0
  fi
fi

pdflatex accumulo_user_manual.tex 
pdflatex accumulo_user_manual.tex 
pdflatex accumulo_user_manual.tex 
pdflatex accumulo_user_manual.tex 
pdflatex accumulo_user_manual.tex && (
find . -name '*.aux' -print | xargs rm -f 
rm -f *.log
mv accumulo_user_manual.pdf ../..
)
