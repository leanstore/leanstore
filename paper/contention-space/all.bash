#!/bin/bash -f

#find -L experiments -name "*.r" -exec echo {} \; -exec Rscript {} \;
find -L experiments -name "*.pdf" -exec echo {} \; -exec cp {} tex/figures/ \;
