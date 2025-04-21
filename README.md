# Avaliação de Impacto da Estratégia de Cupons na Retenção de Usuários

Este repositório contém o código e a análise para avaliar o impacto de uma estratégia de cupons sobre a retenção de usuários. O objetivo é analisar se o uso de cupons está influenciando a taxa de retenção, o ticket médio e a taxa de conversão de usuários.

## Estrutura do Projeto
- `case_ifood - analise_campanha_cupons.py`: Notebook com modelagem e análise exploratória.
- `Relatório - Campanha de Cupons - Teste AB.pdf`: Relatório da análise.
- `README.md`: Este arquivo com a documentação do projeto.

## Pré-requisitos

Antes de executar o projeto, certifique-se de ter os seguintes pacotes instalados:

- Python 3.x
- Bibliotecas Python:
  - pandas
  - numpy
  - statsmodels
  - math

Você pode instalar as dependências utilizando o `pip`:

```bash
pip install -r requirements
```

## Siga os passos abaixo para acessar/processar o notebook:

Etapa 1: Baixar e descompactar os arquivos

1. Faça upload dos seguintes arquivos (baixados manualmente dos links abaixo):

- orders_data	[order.json.gz](https://data-architect-test-source.s3-sa-east-1.amazonaws.com/order.json.gz)
- consumers_data	[consumer.csv.gz](https://data-architect-test-source.s3-sa-east-1.amazonaws.com/consumer.csv.gz)
- merchants_data	[restaurant.csv.gz](https://data-architect-test-source.s3-sa-east-1.amazonaws.com/restaurant.csv.gz)
- ab_test_ref_data	[ab_test_ref.tar.gz](https://data-architect-test-source.s3-sa-east-1.amazonaws.com/ab_test_ref.tar.gz)

2. Extraia os arquivos .tar.gz localmente e envie os CSVs/JSONs para o Databricks, salvando as tabelas conforme o nome do passo 1

3. Altere o caminho das fontes, se necessário, no notebook.

   
## Acesso Rápido
Caso queira acessar o notebook como visualização, beem como todos os resultados sem necesside de rodar, [clique aqui](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/2110729935403588/2434208335637225/4474531956897067/latest.html)

## Descrição das tabelas de dados disponíveis  

#Pedidos (order.json)  
https://data-architect-test-source.s3-sa-east-1.amazonaws.com/order.json.gz 
Contém dados de cerca de 3.6 milhões de pedidos realizados entre dez/18 e  jan/19. Cada pedido possui um order_id e os seguintes atributos complementa res:  
- cpf (string): Cadastro de Pessoa Física do usuário que realizou o pedi do  
- customer_id (string): Identificador do usuário  
- customer_name (string): Primeiro nome do usuário  
- delivery_address_city (string): Cidade de entrega do pedido  - delivery_address_country (string): País da entrega  
- delivery_address_district (string): Bairro da entrega  
- delivery_address_external_id (string): Identificador do endereço  de entrega  
- delivery_address_latitude (float): Latitude do endereço de entre ga  
- delivery_address_longitude (float): Longitude do endereço de  entrega  
- delivery_address_state (string): Estado da entrega  
- delivery_address_zip_code (string): CEP da entrega  
- items (array[json]): Itens que compõem o pedido, bem como informa ções complementares como preço unitário, quantidade, etc. 
- merchant_id (string): Identificador do restaurante  
- merchant_latitude (float): Latitude do restaurante  
- merchant_longitude (float): Longitude do restaurante  
- merchant_timezone (string): Fuso horário em que o restaurante está  localizado  
- order_created_at (timestamp): Data e hora em que o pedido foi cri ado  
- order_id (string): Identificador do pedido  
- order_scheduled (bool): Flag indicando se o pedido foi agendado ou  não (pedidos agendados são aqueles que o usuário escolheu uma data  e hora para a entrega)  
- order_total_amount (float): Valor total do pedido em Reais  - origin_platform (string): Sistema operacional do dispositivo do  usuário  
- order_scheduled_date (timestamp): Data e horário para entrega do  pedido agendado

#Usuários (consumers.csv)  
https://data-architect-test-source.s3-sa-east-1.amazonaws.com/ consumer.csv.gz 
Contém dados de cerca de 806k usuários do iFood. Cada usuário possui um  customer_id e os seguintes atributos complementares:  
- customer_id (string): Identificador do usuário  
- language (string): Idioma do usuário  
- created_at (timestamp): Data e hora em que o usuário foi criado  - active (bool): Flag indicando se o usuário está ativo ou não 
- customer_name (string): Primeiro nome do usuário  
- customer_phone_area (string): Código de área do telefone do usuá rio  
- customer_phone_number (string): Número do telefone do usuário  

#Merchants (restaurant.csv)  
https://data-architect-test-source.s3-sa-east-1.amazonaws.com/ restaurant.csv.gz 
Contém dados de cerca de 7k restaurantes do iFood. Cada restaurante possui  um id e os seguintes atributos complementares:  
- id (string): Identificador do restaurante  
- created_at (timestamp): Data e hora em que o restaurante foi criado  - enabled (bool): Flag indicando se o restaurante está ativo no iFood ou  não  
- price_range (int): Classificação de preço do restaurante  
- average_ticket (float): Ticket médio dos pedidos no restaurante  - delivery_time (float): Tempo padrão de entrega para pedidos no res taurante  
- minimum_order_value (float): Valor mínimo para pedidos no restau rante  
- merchant_zip_code (string): CEP do restaurante  
- merchant_city (string): Cidade do restaurante  
- merchant_state (string): Estado do restaurante  
- merchant_country (string): País do restaurante 

#Marcação de usuários que participaram do teste A/B (ab_- test_ref.csv)  
https://data-architect-test-source.s3-sa-east-1.amazonaws.com/ ab_test_ref.tar.gz 
Contém uma marcação indicando se um usuário participou do teste A/B em  questão. Assim como a base de usuários, cada usuário possui um customer_id.  Os campos são:  
- customer_id (string): Identificador do usuário  
- is_target (string): Grupo ao qual o usuário pertence ('target' ou  'control').
