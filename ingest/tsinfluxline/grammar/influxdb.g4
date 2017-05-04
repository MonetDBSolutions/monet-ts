grammar influxdb;
//sys.testing,location=us a=12i,b="hello" 1493109338000

lines
    : line+
    ;

line
    : metric tags space values timestamp? (newline | EOF)
    ;

metric
    : INFLUXWORD
    ;

tags
    : tags ',' ttype
    | ',' ttype
    |
    ;

ttype
    : INFLUXWORD '=' INFLUXWORD
    ;

values
    : values ',' vtype
    | vtype
    ;

vtype
    : INFLUXWORD '=' INFLUXWORD
    | INFLUXWORD '=' INFLUXWORD
    | INFLUXWORD '=' INFLUXWORD
    | INFLUXWORD '=' INFLUXSTRING
    ;

timestamp
    : space INFLUXWORD
    ;

space
    :  ' '
    ;

newline
    :  '\r'? '\n'
    ;

INFLUXWORD: ('a'..'z' | 'A'..'Z' | '_' | '-' | '.' | '/' | '0'..'9')+;


INFLUXSTRING
    : '"' (~('"' | '\\') | '\\' ('"' | '\\'))+ '"'
    ;
