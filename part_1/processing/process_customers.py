import json
import logging
import typing

import apache_beam as beam
import pyarrow
from apache_beam.io import fileio, parquetio
from apache_beam.io.textio import ReadFromText, WriteToText
from apache_beam.io.gcp import bigquery
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    SetupOptions,
    StandardOptions,
)

# from google.cloud import bigquery as bq
# import gcsfs

# from apitools.clients import bigquery as bq

from apache_beam.io.gcp.bigquery import parse_table_schema_from_json

import argparse

_options = PipelineOptions(
    runner="DataflowRunner",
    project="precis-digital-case-interview",
    job_name="data-processing",
    temp_location="gs://precis-digital-case-interview-landing-bucket/temp/",
    region="australia-southeast1",
    streaming=False,
)


def json_to_schema(json_obj: dict) -> str:
    schema_str = []
    
    for key in json_obj["fields"]:
        _k = key["name"]
        _t = key["type"]
        schema_str.append(f"{_k}:{_t}")

    return ",".join(schema_str)



class CloudStorageToBigQuery:
    """A helper class which contains the logic to translate the file into
    a format BigQuery will accept.

    This example uses side inputs to join two datasets together.
    """

    def process_row(self, x, schema):
        values = x.split(",")
        fields = [x["name"] for x in schema["fields"]]

        result = {}

        for i in range(len(fields)):
            result[fields[i]] = values[i]
        
        logging.info(x)
        logging.info(result)

        return result


def run(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument("--file_location", dest="file_location", required=True)
    parser.add_argument("--schema_location", dest="schema_location", required=True)
    parser.add_argument("--output_table", dest="output_table", required=True)

    known_args, pipeline_args = parser.parse_known_args(argv)

    file_location = known_args.file_location
    schema_location = known_args.schema_location
    output_table = known_args.output_table

    cloud_storage_to_bq = CloudStorageToBigQuery()

    with open("./schemas/customers.json") as f:
        schema_dict = json.load(f)
        table_schema = json_to_schema(schema_dict)

    p = beam.Pipeline(options=_options)
    (
        p
        | "Read CSV file from cloud storage"
        >> ReadFromText(
            file_pattern=file_location,
            skip_header_lines=True
        )
        | 'CSV Row To BigQuery Row' 
        >> beam.Map(lambda s: cloud_storage_to_bq.process_row(s, schema_dict))
        | "Write to BigQuery" 
        >> bigquery.WriteToBigQuery(
            table=f"precis-digital-case-interview:part_1.{output_table}",
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
        )
    )

    p.run().wait_until_finish()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()