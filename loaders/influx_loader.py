import sys

BUFFER_SIZE = 1000
tables = dict()

def flush():
    print(tables)

def parse(line):
    groups = line.split()
    metric_name = groups[0].split(',')[0]
    tags = dict()
    # get the tag columns
    for t in groups[0].split(',')[1:]:
        key = t.split('=')[0]
        value = t.split('=')[1]
        tags[key] = [value]
    # get the value columns
    values = dict()
    for v in groups[1].split(','):
        key = v.split('=')[0]
        value = v.split('=')[1]
        values[key] = [value]
    ts = groups[2]
    return { 'metric_name': metric_name, 'tags': tags, 'values': values, 'ts': ts}

def updateTables(ts_point):
    """
        NOTE: First encauntered line for each series donates the schema.
            This is a temp solution.
    """
    table = None
    metric = ts_point['metric_name']
    if tables.has_key(metric):
        table = tables[metric]
        #update tags
        tags = table['tags']
        for k, v in tags.iteritems():
            table['tags'][k] += v
        #update values
        values = table['values']
        for k, v in values.iteritems():
            table['values'][k] += v
        table.time.append(ts_point['ts'])
    else:
        table = {'tags': ts_point['tags'], 'values': ts_point['values'], 'time': [ts_point['ts']]}
        tables[metric] = table
    return table




def main():
    count = 0
    for line in sys.stdin:
        updateTables(parse(line))
        count += 1
        if count == BUFFER_SIZE:
            flush()
    flush()

if __name__ == "__main__":
    main()
