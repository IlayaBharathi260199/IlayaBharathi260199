Complex Data
============
Complex data is nothing but,  "hierarchy of data or nested data",
After flattening the data , we can do transformations by using Spark DSL

Array
=====
If anywhere we see "Array" as a datatype in our Schema, we can use "explode()" to flatten Array.

struct
======
If anywhere we see "Struct" as a datatype in our Schema, we can use "."(Dot) to flatten struct.
If you wanna select all fields from one struct we can use .*
