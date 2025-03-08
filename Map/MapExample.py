
import apache_beam as beam
# Output PCollection
# class Output(beam.PTransform):
#     class _OutputFn(beam.DoFn):
#         def __init__(self, prefix=''):
#             super().__init__()
#             self.prefix = prefix

#         def process(self, element):
#             print(self.prefix+str(element))
#     def __init__(self, label=None,prefix=''):
#         super().__init__(label)
#         self.prefix = prefix

#     def expand(self, input):
#         input | beam.ParDo(self._OutputFn(self.prefix))


def printLog(element):
    print(f"Element -> {element}")
    return element



# with beam.Pipeline() as p:
#   (p | beam.Create([10, 20, 30, 40, 50])
#     # Lambda function that returns an element by multiplying 5
#      | beam.Map(lambda num: num * 5)
#      | Output())

with beam.Pipeline() as p:
    input = p | 'Create words' >> beam.Create(['Hello', 'World', 'How', 'are', 'you'])

    uppercase_words = input | 'Convert to uppercase' >> beam.Map(lambda word: word.upper()) | 'Print elements' >> beam.Map(printLog)
    
    #uppercase_words | 'Print elements' >> beam.Map(printLog)
   # uppercase_words >> beam.Map(print(uppercase_words))
