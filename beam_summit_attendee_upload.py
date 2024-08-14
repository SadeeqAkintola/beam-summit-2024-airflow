import csv
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
from apache_beam.io.fileio import MatchFiles, ReadMatches
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
import datetime
import logging

class ReadAndValidateCSV(beam.DoFn):
    def process(self, file):
        file_path = file.metadata.path
        logging.info(f"Processing file: {file_path}")
        try:
            with beam.io.filesystems.FileSystems.open(file_path) as f:
                text_io = f.read().decode('utf-8').splitlines()
                reader = csv.reader(text_io)
                header = next(reader, None)
                if header and len(header) == 3:
                    for row in reader:
                        if len(row) == 3:
                            # Prepare data for BigQuery
                            yield {
                                'name': row[0],
                                'email': row[1],
                                'phone_number': row[2],
                                'file_location': file_path  # Add file location here
                            }
                        else:
                            logging.error(f"Invalid row in file {file_path}: {row}")
                else:
                    logging.error(f"Skipping invalid file (wrong number of columns): {file_path}")
        except Exception as e:
            logging.error(f"Error processing file {file_path}: {e}")

class AddTimestamp(beam.DoFn):
    def process(self, element):
        element['timestamp'] = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f UTC')
        return [element]

def run(argv=None):
    pipeline_options = PipelineOptions(argv)
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'beam-summit-2024-airflow'
    #google_cloud_options.job_name = f"beam-summit-2024-run-{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}"
    google_cloud_options.region = 'us-central1'
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
    pipeline_options.view_as(SetupOptions).save_main_session = True

    p = beam.Pipeline(options=pipeline_options)

    input_pattern = 'gs://beam-summit-2024/initiated-runs/*.csv'
    table_schema = {
        'fields': [
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'phone_number', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'file_location', 'type': 'STRING', 'mode': 'NULLABLE'}  # Add schema for file_location
        ]
    }

    (p
     | 'MatchFiles' >> MatchFiles(input_pattern)
     | 'ReadMatches' >> ReadMatches()
     | 'ReadAndValidateCSV' >> beam.ParDo(ReadAndValidateCSV())
     | 'AddTimestamp' >> beam.ParDo(AddTimestamp())
     | 'WriteToBigQuery' >> WriteToBigQuery(
         'beam-summit-2024-airflow:beam_2024_attendees.registrations',
         schema=table_schema,
         create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
         write_disposition=BigQueryDisposition.WRITE_APPEND
     )
    )

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    run()