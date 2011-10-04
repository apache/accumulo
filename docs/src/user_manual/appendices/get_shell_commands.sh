#!/bin/bash

echo "\\chapter{Shell Commands}"
echo
echo "\\begin{alltt}"
echo "help -npquit" | \
../../../../bin/accumulo shell -u foo -p foo --fake | \
#head -11 | \
gawk 'BEGIN{count = 0}{if(count > 9 && substr($0,0,4) != "foo@"){print "\\textbf{"$1"}\n"; system("./get_shell_command.sh \""$1"\""); print "";};count += 1}' |\
sed -e 's/_/\\_/g'
echo
echo "\\end{alltt}"

