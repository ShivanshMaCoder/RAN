import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterProcessingTime, AccumulationMode
import json

class ParseJson(beam.DoFn):
    def process(self, element):
        record = json.loads(element.decode('utf-8'))
        return [record]

class AddKeys(beam.DoFn):
    def process(self, element):
        network_id = element['network_id']
        return [(network_id, element)]

class ComputeMean(beam.CombineFn):
    def create_accumulator(self):
        return (0, 0) 
    
    def add_input(self, accumulator, element):
        sum_accumulator, count_accumulator = accumulator
        sum_values = {key: sum_accumulator.get(key, 0) + value for key, value in element.items() if key != 'network_id'}
        count_accumulator += 1
        return (sum_values, count_accumulator)
    
    def merge_accumulators(self, accumulators):
        sum_accumulator = {}
        count_accumulator = 0
        for (sum_values, count) in accumulators:
            count_accumulator += count
            for key, value in sum_values.items():
                sum_accumulator[key] = sum_accumulator.get(key, 0) + value
        return (sum_accumulator, count_accumulator)
    
    def extract_output(self, accumulator):
        sum_values, count_accumulator = accumulator
        if count_accumulator == 0:
            return {}
        mean_values = {key: value / count_accumulator for key, value in sum_values.items()}
        mean_values['network_id'] = sum_values['network_id']
        return mean_values

def infer_schema(element):
    
    schema = {}
    for key, value in element.items():
        if isinstance(value, int):
            schema[key] = 'INTEGER'
        elif isinstance(value, float):
            schema[key] = 'FLOAT'
        elif isinstance(value, str):
            schema[key] = 'STRING'
        else:
            raise ValueError(f"Unsupported data type: {type(value)} for field {key}")
    return schema

def run():
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        records = (p 
                   | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(subscription='projects/chacha-424909/subscriptions/ran-data-sub')
                   | 'ParseJson' >> beam.ParDo(ParseJson())
                   )

        # Infer schema
        inferred_schema = (records
                           | 'Sample' >> beam.combiners.Sample.FixedSizeGlobally(1)
                           | 'InferSchema' >> beam.Map(lambda elements: infer_schema(elements[0]))
                           )

        def to_bq_schema(inferred_schema):
            schema_str = ', '.join([f"{key}:{dtype}" for key, dtype in inferred_schema.items()])
            return schema_str

        schema_str = inferred_schema | 'ToBQSchema' >> beam.Map(to_bq_schema)

        (records
         | 'WindowInto' >> beam.WindowInto(FixedWindows(10 * 60),
                                           trigger=AfterProcessingTime(10 * 60),
                                           accumulation_mode=AccumulationMode.DISCARDING)
         | 'AddKeys' >> beam.ParDo(AddKeys())
         | 'GroupByKey' >> beam.GroupByKey()
         | 'ComputeMean' >> beam.CombineValues(ComputeMean())
         | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                'chacha-424909.ran.ran-time-data',
                schema=lambda _: schema_str.get(),
                schema_side_inputs=(schema_str,),
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        )

if __name__ == '__main__':
    run()
