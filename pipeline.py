import apache_beam as beam
import logging
import csv
class ExtractWordsFn(beam.DoFn):

  def process(self, element):
      with open('all_alarm.log', 'r') as f_input, open('test.csv', 'w') as f_output:
          csv_output = csv.writer(f_output)
          cols = ["applainceName", "tenantName", "alarmType", "alarmKey", "generateTime", "applianceId", "vsnId",
                  "tenantId", "alarmCause", "alarmClearable", "alarmClass", "alarmKind", "alarmEventType",
                  "alarmSeverity", "alarmOwner", "alarmSeqNo", "alarmText", "siteName"]
          csv_output.writerow(cols)

          for row in csv.reader(f_input, delimiter=','):
              row = [c for c in row if c.find('=') != -1]
              # Convert remaining columns into a dictionary
              entries = {c.split('=')[0].strip(): c.split('=')[1].strip() for c in row}
              csv_output.writerow([entries.get(c, "") for c in cols])


  if __name__=='__main__':
      p=beam.Pipeline(argv=sys.argv)
      (p
      |beam.io.ReadFromText('gs://anamika_data')
      |beam.FlatMap(lambda line: E )
       )
      p.run()