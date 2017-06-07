DROP SCHEMA timetrails;
CREATE SCHEMA timetrails;

CREATE TABLE timetrails.metrics (
     name  string PRIMARY KEY,
     auth  string,        --(for later) The owner ticket for this metric
     credit integer,     --(for later)The volume of data allowed for this metric
     precision string , --time stamp precision {millisecond,second,minute,hour,day,month,year, null}
     retention string,  --time stamp interval {millisecond,second,minute,hour,day,month,year, null}
     threshold integer, --max rows delayed in the Guardian cache
     heartbeat integer, -- maximum delay (in ms) before forwarding
     frozen boolean,    --once defined its structure can not be changed
     title string,            --informative title 
     description string --short explanation
);

--Return the names of all measurments in timetrails schema
CREATE FUNCTION timetrails.measurments()
RETURNS TABLE (measurment string)
BEGIN
    RETURN 
        SELECT name 
        FROM sys.tables 
        WHERE schema_id=(SELECT id as id FROM sys.schemas WHERE name='timetrails') AND name<>'metrics';
END;

--Return the names of all known metric relations
CREATE FUNCTION timetrails.metrics()
RETURNS TABLE (metric string)
BEGIN
    RETURN SELECT m.name FROM timetrails.metrics m ORDER BY m.name;
END;

--Return the tags associated with a metric relation
CREATE FUNCTION timetrails.tags(metric string)
RETURNS TABLE (colname string)
BEGIN 
   RETURN SELECT o.name AS colname 
       FROM sys.objects o, sys.tables t, sys.keys k 
       WHERE o.id = k.id AND k.table_id = t.id AND t.name = metric AND o.name<>'ticks' ORDER BY o.nr;
END;

--Return the measure names for a metric relation
CREATE FUNCTION timetrails.fields(metric string)
RETURNS TABLE (colname string)
BEGIN 
   RETURN SELECT c.name 
        FROM sys.columns c, sys.tables t 
        WHERE t.name = metric AND t.id = c.table_id AND 
              c.name NOT IN (SELECT o.name AS colname
                        FROM sys.objects o, sys.tables tt, sys.keys k WHERE o.id = k.id AND k.table_id = tt.id AND tt.name = metric);
END;

--Return the preferred message layout and their type
CREATE FUNCTION timetrails.getLayout(metric string)
RETURNS TABLE( name string, type string)
BEGIN
   RETURN SELECT c.name, c.type FROM timetrails.metrics m, sys.tables t, sys.columns c WHERE m.name = metric AND t.name = m.name and c.table_id= t.id;
END;
--Return the time precision for a metric relation
CREATE FUNCTION timetrails.getPrecision(metric string)
RETURNS string
BEGIN
   RETURN SELECT m.precision FROM timetrails.metrics m WHERE m.name = metric;
END;
--Return the retention period of a metric relation
CREATE FUNCTION timetrails.getRetention(metric string)
RETURNS string
BEGIN
   RETURN SELECT m.retention FROM timetrails.metrics m WHERE m.name = metric;
END;
--Return the title annotation
CREATE FUNCTION timetrails.getTitle(metric string)
RETURNS string
BEGIN
   RETURN SELECT m.title FROM timetrails.metrics m WHERE m.name = metric;
END;
--Return the short help on a metric relation
CREATE FUNCTION timetrails.getDescription(metric string)
RETURNS string
BEGIN
   RETURN SELECT m.description FROM timetrails.metrics m WHERE m.name = metric;
END;

