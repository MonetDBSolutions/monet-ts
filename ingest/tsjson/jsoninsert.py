from typing import Dict, Any


from ingest.monetdb.mapiconnection import get_mapi_connection
from ingest.monetdb.naming import TIMESTAMP_COLUMN_NAME, get_default_timestamp_value


def insert_json_values(json: Dict[Any, Any]) -> None:
    time_to_input = get_default_timestamp_value()  # if the timestamp is missing
    inserts = []

    for entry in json['values']:
        values = []

        for key, value in entry.items():
            if type(value) == str:
                values.append(value)
            else:
                values.append(str(value))

        if TIMESTAMP_COLUMN_NAME not in entry.keys():
            values.append(time_to_input)

        inserts.append('|'.join(values))

    metric_name = "\"%s\".\"%s\"" % (json['schema'], json['stream'])

    # "COPY 2 RECORDS INTO test FROM STDIN;\n44444444|AL\n55555555|JAFFRI"
    get_mapi_connection().insert_points(metric_name, len(json['values']), '\n'.join(inserts))
