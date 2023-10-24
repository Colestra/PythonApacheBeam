import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = {
  'project': 'curso-data-flow-368611',
  'runner': 'DataflowRunner',
  'region': 'southamerica-east1',
  'staging_location': 'gs://curso-apache-beam-colestra/temp',
  'temp_location': 'gs://curso-apache-beam-colestra/temp',
  'template_location': 'gs://curso-apache-beam-colestra/template/batch_job_df_gcs_voos'
}

serviceAccount = "chave-curso-apache-beam.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
p1 = beam.Pipeline(options=pipeline_options)

class filtro(beam.DoFn):
  def process(self, registro):
    if int(registro[8]) > 0:
      return [registro]

Tempo_Atrasos = (
    p1
    |"Importar Dados Atraso" >> beam.io.ReadFromText("gs://curso-apache-beam-colestra/entrada/voos_sample.csv", skip_header_lines=1)
    |"Separar por Vírgulas Atraso" >> beam.Map(lambda registro: registro.split(','))
    |"Pegar voos de Los Angeles atraso" >> beam.ParDo(filtro())
    |"Criar par atraso" >> beam.Map(lambda registro: (registro[4], int(registro[8])))
    |"Somar por key atraso">> beam.CombinePerKey(sum)
)

Qtd_Atrasos = (
    p1
    |"Importar Dados qtd" >> beam.io.ReadFromText("gs://curso-apache-beam-colestra/entrada/voos_sample.csv", skip_header_lines=1)
    |"Separar por Vírgulas qtd" >> beam.Map(lambda registro: registro.split(','))
    |"Pegar voos de Los Angeles qtd" >> beam.Filter(lambda registro: int(registro[8]) > 0)
    |"Criar par qtd" >> beam.Map(lambda registro: (registro[4], int(registro[8])))
    |"Contar por key qtd" >> beam.combiners.Count.PerKey()
)

tabela_atrasos = (
    {"Qtd_Atrasos": Qtd_Atrasos, "Tempo_Atrasos": Tempo_Atrasos}
    |"Group By" >> beam.CoGroupByKey()
    |"Saida para GCP" >> beam.io.WriteToText(r'gs://curso-apache-beam-colestra/saida/voos_atrasados_qtd_saidas.csv')

)

p1.run()