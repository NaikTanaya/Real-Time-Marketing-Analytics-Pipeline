import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
import redis

class ParseTransactionDoFn(beam.DoFn):
    """Parses incoming JSON streaming strings into Python dict objects."""
    def process(self, element):
        try:
            record = json.loads(element.decode('utf-8') if isinstance(element, bytes) else element)
            yield record
        except Exception as e:
            logging.error(f"Error parsing transaction event: {e}")

class AggregateMetricsCombineFn(beam.CombineFn):
    """Combines metrics over fixed windows to compute avg latency and success rate."""
    def create_accumulator(self):
        return {'total_latency': 0.0, 'count': 0, 'success_count': 0}

    def add_input(self, accumulator, input_element):
        accumulator['total_latency'] += float(input_element.get('latencyMs', 0.0))
        accumulator['count'] += 1
        if input_element.get('status') == 'SUCCESS':
            accumulator['success_count'] += 1
        return accumulator

    def merge_accumulators(self, accumulators):
        merged = {'total_latency': 0.0, 'count': 0, 'success_count': 0}
        for acc in accumulators:
            merged['total_latency'] += acc['total_latency']
            merged['count'] += acc['count']
            merged['success_count'] += acc['success_count']
        return merged

    def extract_output(self, accumulator):
        count = accumulator['count']
        if count == 0:
            return {'avg_latency_ms': 0.0, 'success_rate': 0.0, 'total_transactions': 0}

        avg_latency = accumulator['total_latency'] / count
        success_rate = (accumulator['success_count'] / count) * 100.0
        return {
            'avg_latency_ms': float(avg_latency),
            'success_rate': float(success_rate),
            'total_transactions': int(count)
        }

class FeedbackRoutingRulesDoFn(beam.DoFn):
    """Evaluates computed metrics and updates Redis dynamic routing rules."""
    def __init__(self, redis_host='localhost', redis_port=6379):
        self.redis_host = redis_host
        self.redis_port = redis_port

    def setup(self):
        self.client = redis.Redis(host=self.redis_host, port=self.redis_port, decode_responses=True)

    def process(self, element):
        avg_latency = element.get('avg_latency_ms', 0.0)

        # Dynamic feedback rule: switch gateway if average latency exceeds threshold
        if avg_latency > 100.0:
            self.client.set("ROUTING_RULE:PREFERRED_GATEWAY", "GATEWAY_SECONDARY")
        else:
            self.client.set("ROUTING_RULE:PREFERRED_GATEWAY", "GATEWAY_PRIMARY")

        yield element

def run():
    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'your-gcp-project-id'
    google_cloud_options.job_name = 'marketing-analytics-pipeline'
    google_cloud_options.staging_location = 'gs://your-bucket/staging'
    google_cloud_options.temp_location = 'gs://your-bucket/temp'

    options.view_as(StandardOptions).runner = 'DataflowRunner'

    table_spec = 'your-gcp-project-id:marketing_analytics.transaction_metrics'

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Read From PubSub' >> beam.io.ReadFromPubSub(subscription='projects/your-gcp-project-id/subscriptions/tx-sub')
            | 'Parse JSON' >> beam.ParDo(ParseTransactionDoFn())
            | '1-Minute Fixed Window' >> beam.WindowInto(FixedWindows(60))
            | 'Aggregate Metrics' >> beam.CombineGlobally(AggregateMetricsCombineFn()).without_defaults()
            | 'Update Redis Dynamic Feedback' >> beam.ParDo(FeedbackRoutingRulesDoFn())
            | 'Write To BigQuery' >> beam.io.WriteToBigQuery(
                table_spec,
                schema='avg_latency_ms:FLOAT, success_rate:FLOAT, total_transactions:INTEGER',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
