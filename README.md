# Leanstore DB
## Compiling
Install dependencies:
`sudo apt-get install cmake libaio-dev libtbb-dev`

`make debug && cd debug && cmake -DCMAKE_BUILD_TYPE=Debug ..`

and/or

`make release && cd release && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo ..`

## Naming conventions:
functionName
ClassName
ClassName.hpp
ClassName.cpp
variable_name
directory-name
file_name

CONSTANT_NAME

class name always in singular
variable name can be plural if it is a collection
