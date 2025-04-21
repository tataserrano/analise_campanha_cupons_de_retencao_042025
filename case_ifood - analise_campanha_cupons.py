# Databricks notebook source
# MAGIC %md
# MAGIC ###Imports

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType
from pyspark.sql.functions import count, countDistinct, sum, avg, col, from_json, udf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from scipy.stats import t
import math
from scipy.stats import ttest_ind
from statsmodels.stats.proportion import proportions_ztest


# COMMAND ----------

# MAGIC %md
# MAGIC #### Mapeamento de Bases de Dados

# COMMAND ----------

#File Locations
df_orders_path = 'hive_metastore.default.orders_data'
df_consumers_path = 'hive_metastore.default.consumers_data'
df_merchants_path = 'hive_metastore.default.merchants_data'
df_ab_test_path = 'hive_metastore.default.ab_test_ref_data'

#Load data from files path
df_orders = spark.read.table(df_orders_path)
df_consumers = spark.table(df_consumers_path)
df_merchants = spark.table(df_merchants_path)
df_ab_test = spark.table(df_ab_test_path)

# Print basic info
print("Data shapes:")
print(f"Orders: {df_orders.count()} rows, {len(df_orders.columns)} columns")
print(f"Consumers: {df_consumers.count()} rows, {len(df_consumers.columns)} columns")
print(f"Merchants: {df_merchants.count()} rows, {len(df_merchants.columns)} columns")
print(f"AB: {df_merchants.count()} rows, {len(df_merchants.columns)} columns")

# COMMAND ----------


df_total = (
    df_orders.alias('orders')
    .join(df_ab_test,
          'customer_id',
          'left')
    .join(df_merchants.alias('merchants'),
          F.col('orders.merchant_id') == F.col('merchants.id'),
          'left')
    .join(df_consumers.alias('consumers'),
          'customer_id',
          'left')
    .drop(F.col('merchants.id'))
)

display(df_total)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Análise - Campanha de Tickets - Teste A/B

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. No iFood, várias áreas utilizam testes A/B para avaliar o impacto de  ações em diferentes métricas. Esses testes permitem validar hipóteses  de crescimento e a viabilidade de novas funcionalidades em um grupo  restrito de usuários. Nos dados fornecidos nesse case você encontrará  uma marcação de usuários, separando-os entre grupo teste e controle  de uma campanha de cupons, que disponibilizou para os usuários do  grupo teste um cupom especial.  
# MAGIC - a) Defina os indicadores relevantes para mensurar o sucesso da  campanha e analise se ela teve impacto significativo dentro do  período avaliado.  
# MAGIC - b) Faça uma análise de viabilidade financeira dessa iniciativa  como alavanca de crescimento, adotando as premissas que julgar necessárias (explicite as premissas adotadas).  
# MAGIC - c) Recomende oportunidades de melhoria nessa ação e desenhe uma nova proposta de teste A/B para validar essas hipóteses. 

# COMMAND ----------

# MAGIC %md
# MAGIC ###### a) Definindo indicadores relevantes e análisando impacto

# COMMAND ----------

# DBTITLE 1,Impacto no Ticket Médio

# Agrupar por cliente
df_metricas_clientes = (
    df_total.groupBy("customer_id", "is_target")
    .agg(
        count("order_id").alias("n_pedidos"),
        sum("order_total_amount").alias("gasto_total"),
        avg("order_total_amount").alias("ticket_medio")
    )
)
display(df_metricas_clientes)
df_metricas_pd = df_metricas_clientes.toPandas()


# Separar os grupos
grupo_teste = df_metricas_pd[df_metricas_pd['is_target'] == 'target']
grupo_controle = df_metricas_pd[df_metricas_pd['is_target'] == 'control']

# Teste t para ticket médio
t_stat, p_valor = ttest_ind(grupo_teste["ticket_medio"], grupo_controle["ticket_medio"], equal_var=False)

print(f"p-valor (ticket médio): {p_valor}")



# COMMAND ----------

# DBTITLE 1,Quem passou a comprar por ter recebido cupom

# Identificar quem fez ao menos 1 pedido
df_conversao = (
    df_total.select("customer_id", "is_target")
    .dropDuplicates()
    .groupBy("is_target")
    .count()
    .withColumnRenamed("count", "clientes_com_pedido")
)

# Total de clientes em cada grupo (com base no df_ab_test)
df_total_clientes = df_ab_test.groupBy("is_target").count().withColumnRenamed("count", "total_clientes")

# Unir os dados
df_conv_final = df_total_clientes.join(df_conversao, on="is_target", how="left").fillna(0)

# Coletar resultados
conv_pd = df_conv_final.toPandas()
grupo_teste = conv_pd[conv_pd["is_target"] == "target"]
grupo_controle = conv_pd[conv_pd["is_target"] == "control"]

# Valores para o teste z
successes = [int(grupo_teste["clientes_com_pedido"]), int(grupo_controle["clientes_com_pedido"])]
totals = [int(grupo_teste["total_clientes"]), int(grupo_controle["total_clientes"])]

# Teste z de proporções
z_stat, p_valor_conv = proportions_ztest(successes, totals)

print(f"Taxa de conversão - Grupo Teste: {successes[0] / totals[0]:.2%}")
print(f"Taxa de conversão - Grupo Controle: {successes[1] / totals[1]:.2%}")
print(f"p-valor (conversão): {p_valor_conv}")

# COMMAND ----------

# MAGIC %md
# MAGIC ######b) Analise de viabilidade financeira
# MAGIC
# MAGIC **Premissas Adotadas**
# MAGIC - **Valor do Cupom:** O valor do cupom concedido aos clientes do grupo teste é de R$10,00.
# MAGIC - **Margem de Lucro:** A margem de lucro da empresa sobre o valor transacionado pelos clientes é de 20%.
# MAGIC - **Gasto Adicional Estimado:** A suposição é que os clientes do grupo teste que receberam o cupom gastaram o mesmo valor médio do grupo controle, já que o teste de ticket médio mostrou que não houve diferença entre os grupos.
# MAGIC - **Número de Cupons Usados:** A suposição é que todos os clientes do grupo teste usaram o cupom. O cálculo de cupons usados deve ser feito com base na coluna `is_target` para filtrar apenas os clientes que pertencem ao grupo teste.

# COMMAND ----------

# Definindo as premissas
valor_cupom = 10  # valor do cupom em reais
margem_lucro_percentual = 0.20  # margem de lucro de 20%

# Calculando o número de cupons usados (grupo teste)
cupons_usados = df_total.filter(df_total.is_target == 'target').count()

# Ticket médio do grupo controle
ticket_medio_controle = (
    df_total
    .filter(df_total.is_target == 'control')
    .agg(F.avg('order_total_amount').alias('ticket_medio_controle'))
    .collect()[0]['ticket_medio_controle']
)
# Gasto adicional estimado (sem aumento no ticket, usa-se o ticket do controle)
transacional_estimado = cupons_usados * ticket_medio_controle

# Custo total da campanha (valor do cupom * número de cupons usados)
custo_campanha = cupons_usados * valor_cupom

# Cálculo do lucro adicional gerado (gasto adicional * margem de lucro)
lucro_adicional = transacional_estimado * margem_lucro_percentual

# Comparação entre lucro e custo da campanha
print(f"Transacional Estimado: R${transacional_estimado:.2f}")
print(f"Custo Total da Campanha: R${custo_campanha:.2f}")
print(f"Lucro Adicional Gerado: R${lucro_adicional:.2f}")

# Viabilidade
if lucro_adicional > custo_campanha:
    print("A campanha é financeiramente viável!")
else:
    print("A campanha não é financeiramente viável.")

# COMMAND ----------

# MAGIC %md
# MAGIC ######c) Oportunidades de Melhoria Identificadas
# MAGIC **Falta de impacto no ticket médio, a campanha não gerou um aumento significativo no ticket médio. Isso pode indicar que:**
# MAGIC
# MAGIC - O cupom foi usado para pedidos que seriam feitos de qualquer forma.
# MAGIC - O valor do cupom (R$10) pode ter sido insuficiente para estimular uma mudança de comportamento.
# MAGIC - Como a taxa de conversão foi 100% para ambos os grupos, é possível que os dados estejam refletindo apenas clientes que realizaram pedidos, o que gera viés de seleção. Talvez a base usada não inclua usuários que não converteram.
# MAGIC - Não há dados específicos que indiquem quem de fato usou o cupom. Isso dificulta a análise de impacto e a mensuração do ROI da campanha.
# MAGIC - O cupom foi oferecido de forma genérica. Segmentar por perfil de cliente (ex: inativos, novos, risco de cancelamento), períodos específicos e localização podem aumentar a efetividade.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. A criação de segmentações permite agrupar usuários de acordo com  características e comportamentos similares, possibilitando criar estratégias direcionadas de acordo com o perfil de cada público, facilitando a  personalização e incentivando o engajamento, retenção, além de otimização de recursos. Segmentações de usuários são muito utilizadas pelos times de Data, mas a área em que você atua ainda não tem segmentos bem definidos e cada área de Negócio utiliza conceitos diferentes. Por isso, você precisa:  
# MAGIC - a) Definir as segmentações que fazem sentido especificamente  para o teste A/B que está analisando.  
# MAGIC - b) Estabelecer quais serão os critérios utilizados para cada segmento sugerido no item a). Utilize os critérios/ferramentas que  achar necessários, mas lembre-se de explicar o racional utilizado na criação.  
# MAGIC - c) Analisar os resultados do teste A/B com base nos segmentos  definidos nos itens a) e b).  
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ######a) Definir as segmentações que fazem sentido especificamente para o teste A/B que está analisando
# MAGIC
# MAGIC Para avaliar de forma mais granular o impacto da campanha, foi segmentado da seguinte forma:
# MAGIC
# MAGIC - **Atividade do usuário:** usuários ativos vs. inativos;
# MAGIC - **Localização do usuário:** agrupamento por estado ou cidade;
# MAGIC - **Valor de consumo:** segmentação por ticket médio histórico – grupos Bronze, Prata, Ouro.
# MAGIC
# MAGIC **Essas segmentações ajudam a responder:**
# MAGIC
# MAGIC - Se a campanha teve impacto diferenciado entre clientes mais engajados (ativos) vs clientes a serem reativados (inativos);
# MAGIC - Se existem regiões onde o cupom tem mais ou menos efeito;
# MAGIC - Se a campanha é mais eficaz para consumidores de maior ou menor ticket médio.

# COMMAND ----------

# MAGIC %md
# MAGIC ######b) Estabelecer quais serão os critérios utilizados para cada segmento sugerido no item a). Utilize os critérios/ferramentas que achar necessários, mas lembre-se de explicar o racional utilizado na criação.

# COMMAND ----------

# MAGIC %md
# MAGIC **1. Atividade**
# MAGIC
# MAGIC - **Ativo:** consumidores com pedidos recentes (ex: nos últimos 30 dias antes da campanha)
# MAGIC - **Inativo:** consumidores sem pedidos nos últimos 30 dias
# MAGIC
# MAGIC **Racional:** identificar se a campanha funciona melhor para reativar clientes inativos ou manter os ativos engajados.
# MAGIC
# MAGIC
# MAGIC **2. Localização**
# MAGIC
# MAGIC - Segmentação por estado ou cidade, com base na coluna `delivery_address_state` ou `delivery_address_city`
# MAGIC
# MAGIC **Racional:** diferentes regiões podem ter comportamentos de consumo distintos, além de diferentes níveis de concorrência ou maturidade da base.
# MAGIC
# MAGIC
# MAGIC **3. Valor de consumo (ticket médio histórico)**
# MAGIC - Calcular o ticket médio por consumidor antes da campanha.
# MAGIC - Classificar consumidores com base em Ticket Médio:
# MAGIC   -   **Ouro:** >= 70   
# MAGIC   -   **Prata:** >= 40 e < 70
# MAGIC   -   **Bronze:** < 40
# MAGIC
# MAGIC **Racional:** entender se o cupom é mais eficaz para incentivar gastos de quem já consome muito ou para atrair os de baixo ticket.

# COMMAND ----------

# MAGIC %md
# MAGIC ######c) Analisar os resultados do teste A/B com base nos segmentos definidos nos itens a) e b).

# COMMAND ----------


# 1. Segmentação por Atividade (Ativos vs Inativos)
df_segmentado_atividade = (
    df_total
    .withColumn("segmento_atividade", 
                F.when(F.col("active") == True, "Ativo")
                .otherwise("Inativo")
                )
    .select("customer_id", "order_total_amount", "delivery_address_city", "is_target", "segmento_atividade")
)

# 2. Segmentação por Ticket Médio (Ouro, Prata, Bronze) – apenas com customer_id e segmento
df_segmentado_ticket = (
    df_total
    .select("customer_id", "order_total_amount")
    .withColumn("segmento_ticket",
                F.when(F.col("order_total_amount") >= 70, "Ouro")
                .when((F.col("order_total_amount") >= 40) & (F.col("order_total_amount") < 70), "Prata")
                .otherwise("Bronze")
                )
    .select("customer_id", "segmento_ticket")
)

# 3. Segmentação por Localização
df_segmentado_localizacao = (
    df_total
    .select("customer_id", "merchant_state")
    .withColumn("segmento_localizacao", 
                col("merchant_state")
                )
    .select("customer_id", "segmento_localizacao")
)

# 4. Unindo segmentações (sem ambiguidade)
df_segmentado = (
    df_segmentado_atividade
    .join(
        df_segmentado_ticket, 
        "customer_id", 
        "left")
    .join(
        df_segmentado_localizacao, 
        "customer_id", 
        "left")
)

# 5. Agrupamento por segmentos + análise
df_segmentado_analise = (
    df_segmentado
    .groupBy("segmento_atividade", "segmento_ticket", "segmento_localizacao", "is_target")
    .agg(
        F.countDistinct("customer_id").alias("total_customers"),
        F.sum("order_total_amount").alias("total_sales"),
        F.avg("order_total_amount").alias("avg_ticket"),
        F.count(F.when(F.col("order_total_amount").isNotNull(), 1)).alias("converted_customers"))
)

# 6. Calculando taxa de conversão
df_segmentado_analise = (
    df_segmentado_analise
    .withColumn("conversion_rate", 
                F.col("converted_customers") / F.col("total_customers") * 100)
)

# 7. Mostrando os resultados por segmento
df_segmentado_analise.show()

# 8. Filtrando apenas grupo teste
df_segmentado_analise_resultado = df_segmentado_analise.filter(F.col("is_target") == "target")
df_segmentado_analise_resultado.show()

# 9. Preparando amostras para exportar e usar t-test fora do PySpark
df_test = (
    df_segmentado
    .filter(F.col("is_target") == "target")
    .select("segmento_atividade", "segmento_ticket", "segmento_localizacao", "order_total_amount")
    )

df_control = (
    df_segmentado
    .filter(F.col("is_target") == "control")
    .select("segmento_atividade", "segmento_ticket", "segmento_localizacao", "order_total_amount")
)


# COMMAND ----------

# Selecionar somente as colunas necessárias
df_export = df_segmentado.select(
    "segmento_atividade", 
    "segmento_ticket", 
    "segmento_localizacao", 
    "is_target", 
    "order_total_amount"
)

# 1. Agrupar por segmento e grupo (target/control) e calcular as estatísticas
agg_stats = (
    df_export
    .groupBy("segmento_atividade", "segmento_ticket", "segmento_localizacao", "is_target")
    .agg(
        F.count("order_total_amount").alias("n"),
        F.avg("order_total_amount").alias("mean"),
        F.variance("order_total_amount").alias("var")
    )
)

# 2. Separar target e control em colunas diferentes (pivot)
pivot_stats = (
    agg_stats
    .groupBy("segmento_atividade", "segmento_ticket", "segmento_localizacao")
    .pivot("is_target", ["target", "control"])
    .agg(
        F.first("n").alias("n"),
        F.first("mean").alias("mean"),
        F.first("var").alias("var")
    )
)

# 3. Calcular o t-stat e p-valor com UDF
def calcular_t_p(n1, m1, v1, n2, m2, v2):
    try:
        if n1 < 2 or n2 < 2:
            return None, None

        # Welch's t-test
        t_stat = (m1 - m2) / math.sqrt((v1 / n1) + (v2 / n2))
        
        # Graus de liberdade
        df = ((v1 / n1 + v2 / n2) ** 2) / (
            ((v1 / n1) ** 2) / (n1 - 1) + ((v2 / n2) ** 2) / (n2 - 1)
        )

        p_val = 2 * (1 - t.cdf(abs(t_stat), df))
        return float(t_stat), float(p_val)
    except:
        return None, None

schema = StructType([
    StructField("t_stat", DoubleType(), True),
    StructField("p_val", DoubleType(), True)
])

udf_ttest = udf(calcular_t_p, schema)

# 4. Aplicar o UDF no DataFrame
resultados_final = pivot_stats.withColumn(
    "ttest",
    udf_ttest(
        F.col("target_n"), F.col("target_mean"), F.col("target_var"),
        F.col("control_n"), F.col("control_mean"), F.col("control_var")
    )
).select(
    "segmento_atividade", "segmento_ticket", "segmento_localizacao",
    "target_mean", "control_mean",
    F.col("ttest.t_stat").alias("t_stat"),
    F.col("ttest.p_val").alias("p_valor")
)

# Exibir resultados
resultados_final.show(truncate=False)


# COMMAND ----------

resultados_final.filter("p_valor IS NOT NULL").orderBy("p_valor").show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Objetivo:**
# MAGIC Avaliar se a distribuição de cupons impacta de forma diferente clientes com alto ou baixo ticket médio, considerando também seu status de atividade (ativo/inativo).
# MAGIC
# MAGIC **Principais achados:**
# MAGIC
# MAGIC - Clientes de ticket mais baixo (ex: segmento Bronze) apresentaram aumento médio de gasto no grupo que recebeu o cupom (target), em comparação ao controle.
# MAGIC - Entre clientes de ticket mais alto (ex: Ouro), o efeito do cupom foi nulo ou negativo, sugerindo que esse perfil pode não ser tão sensível a estímulos promocionais.
# MAGIC - Clientes inativos com ticket baixo mostraram indícios de reativação (gasto maior no grupo target), mas não é possível afirmar que o cupom causou essa mudança.
# MAGIC
# MAGIC **Limitações da análise**:
# MAGIC
# MAGIC - Não há dados sobre a data de envio do cupom, momento do uso, ou ordem entre status de atividade e recebimento.
# MAGIC - Não sabemos se o cupom foi efetivamente utilizado pelos clientes no grupo target.
# MAGIC - Por isso, os resultados refletem associações estatísticas, mas não permitem inferência causal.
# MAGIC
# MAGIC **Próximos passos recomendados**
# MAGIC - Estruturar um experimento controlado (ex: A/B test com marcação de uso do cupom e timestamps).
# MAGIC - Focar em campanhas voltadas a clientes de ticket mais baixo, que demonstram maior sensibilidade a estímulos.
# MAGIC - Explorar formas de medir efetivo uso do cupom e tempo de resposta pós-envio.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Com base na análise que realizou nas questões 1 e 2, sugira os próximos passos que o iFood deve tomar. Lembre-se que você precisa defender suas sugestões para as lideranças de Negócio, por isso não esqueça de incluir uma previsão de impacto (financeiro ou não) caso o  iFood siga com a sua recomendação. Fique à vontade para sugerir melhorias no processo/teste e para propor diferentes estratégias de acordo com cada segmento de usuário.
# MAGIC
# MAGIC - **Estruturar um experimento controlado (A/B Test)**
# MAGIC
# MAGIC   - Implementar um teste A/B com amostragem aleatória entre grupos controle e tratamento. Isso assegura que os dois grupos sejam comparáveis e que qualquer diferença observada nos resultados possa ser atribuída ao cupom, e não a diferenças pré-existentes entre os clientes.
# MAGIC   - Registrar data de envio do cupom, data de uso (se houver), e data do pedido para garantir a análise de impacto com base temporal.
# MAGIC   - Permitir o rastreamento da efetiva utilização do cupom (ex: via código único ou ID de campanha vinculado ao pedido).
# MAGIC
# MAGIC - **Diferenciar usuários que usaram o cupom daqueles que não usaram**
# MAGIC   - Dentro do grupo target, identificar quem recebeu mas não usou o cupom.
# MAGIC   - Isso permitirá avaliar não só a efetividade da campanha, mas também a taxa de engajamento e possíveis barreiras de uso (ex: valor mínimo, prazo, canal).
# MAGIC
# MAGIC - **Segmentar e direcionar campanhas de forma mais eficiente**
# MAGIC   - Focar esforços promocionais em clientes de ticket mais baixo, que demonstraram maior sensibilidade ao estímulo do cupom, especialmente quando estão inativos.
# MAGIC   - Testar segmentações adicionais como frequência de compra, tempo desde o último pedido e canal preferido.
# MAGIC
# MAGIC - **Testar diferentes valores e formatos de cupom**
# MAGIC   - Criar múltiplas variações de valor (ex: R$10, R$20, R$30) e tipo de desconto (percentual vs. fixo). 
# MAGIC   - Avaliar qual combinação gera maior incremento de receita, considerando o custo de incentivo.
# MAGIC   - Analisar também se valores mais altos têm efeito marginal ou produzem canibalização.
# MAGIC   
# MAGIC - **Mensurar tempo de resposta ao cupom**
# MAGIC   - Acompanhar quanto tempo leva entre o envio do cupom e a realização da compra.
# MAGIC   - Isso ajuda a entender a janela de impacto da ação e permite definir validade ideal para futuras campanhas.