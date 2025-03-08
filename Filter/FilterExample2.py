import apache_beam as beam

with beam.Pipeline() as p:
  valid_durations = p | 'Valid durations' >> beam.Create([
      'annual',
      'biennial',
      'perennial',
  ])

  valid_plants = (
      p | 'Gardening plants' >> beam.Create([
          {
              'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'
          },
          {
              'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'
          },
          {
              'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'
          },
          {
              'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'
          },
          {
              'icon': '🥔', 'name': 'Potato', 'duration': 'PERENNIAL'
          },
      ])
      | 'Filter valid plants' >> beam.Filter(
          lambda plant,
          valid_durations: plant['duration'] in valid_durations,
          valid_durations=beam.pvalue.AsIter(valid_durations),
      )
      | beam.Map(print))
