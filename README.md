# case_ifood
📦 ifood-ab-test  
├── data
│   ├── ab_test.csv             # Base com marcação de grupo (teste ou controle)
│   ├── pedidos.csv             # Pedidos realizados por cliente
│   └── clientes.csv            # Dados complementares (opcional)
├── ifood_ab_test_notebook.ipynb  # Notebook principal com a análise completa
├── README.md                  # Este arquivo


🚀 Como utilizar o notebook

1. Requisitos
Python 3.8+

Apache Spark (PySpark)

Pandas

SciPy

Statsmodels

Você pode instalar os pacotes necessários com:

bash
Copiar
Editar
pip install pyspark pandas scipy statsmodels
2. Abrir o notebook
Abra o arquivo ifood_ab_test_notebook.ipynb em um ambiente compatível com Jupyter, como:

Jupyter Notebook

JupyterLab

VS Code

📌 O que o notebook faz
🧪 Etapa A – Análise do Teste A/B
Calcula ticket médio e taxa de conversão por grupo (teste vs controle)

Realiza testes estatísticos (t-teste e z-teste de proporções)

Verifica se há impacto significativo da campanha

💰 Etapa B – Análise de Viabilidade Financeira
Estima lucro e custo da campanha com base em:

Ticket médio

Margem de lucro

Valor do cupom

Verifica se a campanha foi financeiramente viável

🎯 Etapa C – Segmentações e Insights
Cria segmentações de usuários por:

Atividade (ativos vs inativos)

Valor de consumo (bronze, prata, ouro)

Localização (cidade)

Analisa resultados do A/B test dentro de cada segmento

📈 Resultados Esperados
Entendimento se o cupom gerou aumento de conversão ou ticket médio

Avaliação do ROI da campanha

Sugestões de melhorias para futuras campanhas

Nova proposta de teste A/B com segmentações

📎 Observações importantes
A base de pedidos utilizada não possui campo explícito de cupom utilizado. É necessário cruzar com outra base (se disponível) para validar o uso real do cupom.

Todos os testes consideram o uso de Spark para processamento eficiente de grandes volumes de dados.

✨ Próximos passos
Incluir dados de cupons realmente utilizados por pedido

Aplicar modelo de uplift para identificar perfis mais sensíveis à promoção

Testar diferentes valores de cupom em subgrupos

🧑‍💻 Contato
Para dúvidas ou contribuições, entre em contato com:

Nome: [Seu Nome]
Email: seu.email@exemplo.com
LinkedIn: linkedin.com/in/seu-perfil

Se quiser, posso gerar o arquivo .md pronto pra download ou até colar ele num editor pra você salvar direto. Quer que eu faça isso?
