#!/bin/bash -f

find -L . -name "*.r"-exec echo {} \; -exec Rscript {} \;
find -L . -name "*.pdf"  {} \;
