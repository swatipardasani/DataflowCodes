# Code to demonstrate filter
# Code to demonstrate filter
import apache_beam as beam

# Output PCollection
class Output(beam.PTransform):
    class _OutputFn(beam.DoFn):
        def __init__(self, prefix=''):
            super().__init__()
            self.prefix = prefix
        def process(self, element):
            print(self.prefix+str(element))

    def __init__(self, label=None,prefix=''):
        super().__init__(label)
        self.prefix = prefix

    def expand(self, input):
        input | beam.ParDo(self._OutputFn(self.prefix))

# In a range of 1 to 10, filter even numbers 
with beam.Pipeline() as p:
  (p | beam.Create(range(1, 11))
  # The elements filtered with the beam.Filter()
   | beam.Filter(lambda num: num % 2 == 0)
   | Output(prefix='PCollection filtered value: '))
