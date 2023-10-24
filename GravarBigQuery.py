import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = {
  'project': 'curso-data-flow-368611',
  'runner': 'DataflowRunner',
  'region': 'southamerica-east1',
  'staging_location': 'gs://curso-apache-beam-colestra/temp',
  'temp_location': 'gs://curso-apache-beam-colestra/temp',
  'template_location': 'gs://curso-apache-beam-colestra/template/batch_job_df_gcs_big_query',
  'save_main_session': True
}

serviceAccount = "chave-curso-apache-beam.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
p1 = beam.Pipeline(options=pipeline_options)

class filtro(beam.DoFn):
  def process(self, registro):
    if int(registro[8]) > 0:
      return [registro]


def criar_dict_nivel1(registro):
    dict_ = {}
    dict_['airport'] = registro[0]
    dict_['lista'] = registro[1]
    return (dict_)


def desaninhar_dict(registro):
  def expand(key, value):
    if isinstance(value, dict):
      return [(key + '_' + k, v) for k, v in desaninhar_dict(value).items()]
    else:
      return [(key, value)]

  items = [item for k, v in registro.items() for item in expand(k, v)]
  return dict(items)


def criar_dict_nivel0(registro):
  dict_ = {}
  dict_['airport'] = registro['airport']
  dict_['lista_Qtd_Atrasos'] = registro['lista_Qtd_Atrasos'][0]
  dict_['lista_Tempo_Atrasos'] = registro['lista_Tempo_Atrasos'][0]
  return (dict_)



table_schema = 'airport:STRING, lista_Qtd_Atrasos:INTEGER, lista_Tempo_Atrasos:INTEGER'
tabela = 'curso-data-flow-368611.curso_dataflow_voos.curso_dataflow_voos_atraso'

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
    {'Qtd_Atrasos':Qtd_Atrasos,'Tempo_Atrasos':Tempo_Atrasos}
    | beam.CoGroupByKey()
    | beam.Map(lambda registro: criar_dict_nivel1(registro))
    | beam.Map(lambda registro: desaninhar_dict(registro))
    | beam.Map(lambda registro: criar_dict_nivel0(registro))
    | beam.io.WriteToBigQuery(
                              tabela,
                              schema=table_schema,
                              write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                              create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                              custom_gcs_temp_location = 'gs://curso-apache-beam-colestra/temp' )

)

p1.run()