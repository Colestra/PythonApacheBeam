import os
import apache_beam as beam


serviceAccount = "chave-curso-apache-beam.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

p1 = beam.Pipeline()

class filtro(beam.DoFn):
  def process(self, registro):
    if int(registro[8]) > 0:
      return [registro]

Tempo_Atrasos = (
    p1
    |"Importar Dados Atraso" >> beam.io.ReadFromText("voos_sample.csv", skip_header_lines=1)
    |"Separar por Vírgulas Atraso" >> beam.Map(lambda registro: registro.split(','))
    |"Pegar voos de Los Angeles atraso" >> beam.ParDo(filtro())
    |"Criar par atraso" >> beam.Map(lambda registro: (registro[4], int(registro[8])))
    |"Somar por key atraso">> beam.CombinePerKey(sum)
)

Qtd_Atrasos = (
    p1
    |"Importar Dados qtd" >> beam.io.ReadFromText("voos_sample.csv", skip_header_lines=1)
    |"Separar por Vírgulas qtd" >> beam.Map(lambda registro: registro.split(','))
    |"Pegar voos de Los Angeles qtd" >> beam.Filter(lambda registro: int(registro[8]) > 0)
    |"Criar par qtd" >> beam.Map(lambda registro: (registro[4], int(registro[8])))
    |"Contar por key qtd" >> beam.combiners.Count.PerKey()
)

tabela_atrasos = (
    {"Qtd_Atrasos": Qtd_Atrasos, "Tempo_Atrasos": Tempo_Atrasos}
    |"Group By" >> beam.CoGroupByKey()
    |"Saida para GCP" >> beam.io.WriteToText(r'gs://curso-apache-beam-colestra/voos_atrasados_qtd.csv')
)

p1.run()