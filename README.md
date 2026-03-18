# Projeto de Engenharia de Dados e BI — Next Cargas

## Visão geral

Este projeto documenta a construção de um pipeline completo de dados no **Databricks**, seguindo a arquitetura **Medallion**:

- **Bronze**: ingestão bruta
- **Silver**: limpeza, padronização e tipagem
- **Gold**: agregações e KPIs prontos para análise
- **Consumo BI**: conexão com **Power BI**, modelagem em **esquema estrela**, relações e medidas

> **Importante:** os dados usados neste projeto são **dados de teste**, criados e organizados para fins de aprendizado, demonstração técnica e construção de portfólio.

O cenário de negócio é de uma empresa de logística fictícia chamada **Next Cargas**.

---

## Objetivo do projeto

O objetivo foi construir uma solução analítica ponta a ponta que permitisse:

- ingerir arquivos de múltiplos formatos
- tratar problemas de nomes de colunas, tipos e nulos
- consolidar indicadores operacionais e financeiros
- disponibilizar os dados para análise em Power BI
- estruturar um modelo dimensional em estrela para facilitar a exploração do negócio

---

## Contexto técnico

### Ambiente

- Plataforma: **Databricks**
- Catálogo/Schema: `workspace.default`
- Caminho dos arquivos no Volume:
  `/Volumes/workspace/default/next_cargas/`

### Arquivos utilizados

#### 1. `Cadastros.xlsx`
Cadastro de motoristas.

Colunas de origem:

- `SK_Motorista`
- `Motorista`

Quantidade aproximada: **32 motoristas**

#### 2. `fFrete.csv`
Base transacional de fretes.

Colunas de origem:

- `Data`
- `SK_Cliente`
- `SK_Veiculo`
- `Numero Documento Fiscal`
- `Viagem`
- `Cod IBGE`
- `Valor do Frete Líquido`
- `Peso (KG)`
- `Peso (Cubado)`
- `Valor da Mercadoria`

Características técnicas:

- encoding: **UTF-16**
- separador: **tab**

Quantidade aproximada: **92.060 registros**

#### 3. `fKMRodado.xlsx`
Base mensal de operação e custos.

Colunas de origem:

- `Mês`
- `SK_Veiculo`
- `SK_Motorista`
- `Km Percorrido`
- `Litros`
- `Combustível`
- `Manutenção`
- `Custo Fixo`

Quantidade aproximada: **295 registros mensais**

---

## Arquitetura Medallion adotada

### Bronze
Camada destinada a armazenar os dados exatamente como chegam da origem, sem transformações de negócio.

### Silver
Camada em que os dados são limpos, tipados, padronizados e preparados para uso analítico.

### Gold
Camada onde os dados passam a representar indicadores de negócio, agregações e tabelas prontas para consumo em BI.

### Camada de consumo
Views finais e modelo dimensional voltado ao Power BI.

---

# Estrutura do notebook

Nome sugerido do caderno:

```text
pipeline_medallion_next_cargas
```

---

# Bloco a bloco — construção do pipeline

## BLOCO 0 — Configuração do Ambiente e Importação de Bibliotecas

### Objetivo
Preparar o ambiente do Databricks para executar o pipeline.

### O que foi feito
- importação de `pandas`, `numpy` e módulos do `pyspark`
- importação de `unicodedata` e `re`
- criação de função auxiliar para normalizar nomes de colunas

### Por que esse bloco existe
Em projetos reais, ter uma célula inicial padronizada facilita:
- manutenção
- reuso do notebook
- legibilidade
- governança técnica

### Exemplo de responsabilidade da função auxiliar
A função `normalizar_nome_coluna()` foi criada para transformar colunas como:

- `Valor do Frete Líquido` → `valor_do_frete_liquido`
- `Peso (KG)` → `peso_kg`
- `Número Documento Fiscal` → `numero_documento_fiscal`

Isso elimina problemas com:
- acentos
- espaços
- parênteses
- caracteres especiais

---

## BLOCO 1 — Inicialização do Schema e Validação dos Arquivos de Origem

### Objetivo
Garantir que o schema exista e validar a presença dos arquivos no Volume do Databricks.

### O que foi feito
- criação do schema `workspace.default` caso ele ainda não existisse
- definição do schema como contexto padrão
- listagem dos arquivos dentro do caminho do Volume

### Por que esse bloco existe
Ele evita que o pipeline falhe logo no início por problemas simples, como:
- caminho incorreto
- arquivo não encontrado
- schema não existente

### Resultado esperado
Visualização da lista de arquivos disponíveis no Volume.

---

# Camada Bronze

## BLOCO 2 — Ingestão Bronze de Cadastros

### Objetivo
Carregar o arquivo `Cadastros.xlsx` como dado bruto.

### O que foi feito
- leitura do Excel com `pandas`
- conversão para DataFrame Spark
- gravação em Delta na tabela `workspace.default.bronze_cadastros`
- uso de `delta.columnMapping.mode = name`

### Por que usar pandas aqui
O cluster não possuía a biblioteca `com.crealytics.spark.excel`. Como a planilha era pequena, a solução com pandas foi adequada e estável.

### Por que usar `columnMapping.mode = name`
Esse recurso é importante porque a Bronze preserva os nomes originais das colunas, incluindo:
- espaços
- acentos
- caracteres especiais

### Resultado esperado
Tabela Delta bruta com o conteúdo original de cadastro de motoristas.

---

## BLOCO 3 — Ingestão Bronze de Frete

### Objetivo
Carregar o arquivo `fFrete.csv` como dado bruto.

### O que foi feito
- leitura do CSV com:
  - `header = true`
  - `sep = "\t"`
  - `encoding = "UTF-16"`
  - `inferSchema = false`
- gravação em Delta na tabela `workspace.default.bronze_frete`

### Por que `inferSchema = false`
Na Bronze, o objetivo não é interpretar os tipos automaticamente, mas sim preservar o dado bruto o mais fielmente possível.

### Por que esse bloco é importante
Essa é a maior base do projeto. Ela representa os eventos de frete que depois serão consolidados em indicadores de negócio.

---

## BLOCO 4 — Ingestão Bronze de KM Rodado

### Objetivo
Carregar o arquivo `fKMRodado.xlsx` como dado bruto.

### O que foi feito
- leitura com pandas
- conversão para DataFrame Spark
- gravação em Delta na tabela `workspace.default.bronze_km_rodado`

### O que essa tabela representa
Ela concentra uma visão mensal de operação logística:
- quilometragem
- litros consumidos
- combustível
- manutenção
- custo fixo

Essa base será essencial para medir eficiência operacional e custo por motorista.

---

# Camada Silver

## BLOCO 5 — Silver de Cadastros

### Objetivo
Padronizar e limpar a dimensão de motoristas.

### Transformações aplicadas
- normalização dos nomes das colunas
- conversão de `sk_motorista` para inteiro
- remoção de espaços no nome do motorista
- tratamento de `"nan"` como nulo
- remoção de registros sem chave ou nome
- remoção de duplicados

### Tabela gerada
`workspace.default.silver_cadastros`

### Por que esse bloco é importante
A dimensão de motorista precisa estar limpa para:
- suportar joins
- evitar duplicidade em relatórios
- melhorar a qualidade do modelo analítico

---

## BLOCO 6 — Silver de Frete

### Objetivo
Transformar a base transacional de fretes em uma tabela consistente para análise.

### Transformações aplicadas
- padronização de nomes das colunas
- trim em todas as colunas
- tratamento de valores `"nan"`, `"null"` e strings vazias
- conversão da coluna `data`
- conversão de campos numéricos com vírgula decimal
- tratamento de nulos nas métricas
- remoção de registros sem data ou sem veículo
- remoção de duplicidades por chave mínima de negócio

### Tabela gerada
`workspace.default.silver_frete`

### Conversões críticas
Os campos abaixo foram convertidos para numérico:
- `valor_do_frete_liquido`
- `peso_kg`
- `peso_cubado`
- `valor_da_mercadoria`

### Por que esse bloco é importante
Sem essa limpeza, a análise seria comprometida por:
- datas inválidas
- valores como texto
- duplicidades
- inconsistência nos agregados

---

## BLOCO 7 — Silver de KM Rodado

### Objetivo
Preparar a base mensal de custos e operação.

### Transformações aplicadas
- normalização dos nomes das colunas
- trim dos campos
- tratamento de strings inválidas como nulo
- conversão robusta da coluna `mes`
- conversão dos campos numéricos
- preenchimento de nulos com zero nas métricas
- remoção de registros inválidos
- remoção de duplicados mensais por veículo e motorista

### Tabela gerada
`workspace.default.silver_km_rodado`

### Problema tratado
A coluna `mes` podia vir em mais de um formato, como:
- `MM/yyyy`
- `yyyy-MM-dd`

Foi necessário criar uma regra de conversão robusta para evitar erros de parse.

### Por que esse bloco é importante
Essa base será usada para:
- custo por motorista
- desempenho da frota
- eficiência operacional por período

---

# Camada Gold

## BLOCO 8 — Gold de KPIs de Frete por Mês

### Objetivo
Criar uma tabela mensal com os principais indicadores de frete.

### Tabela gerada
`workspace.default.gold_kpi_frete_mes`

### Métricas produzidas
- `quantidade_viagens`
- `quantidade_documentos_fiscais`
- `valor_total_frete_liquido`
- `peso_total_kg`
- `peso_total_cubado`
- `valor_total_mercadoria`
- `ticket_medio_por_viagem`

### Por que essa Gold foi criada
Ela responde perguntas executivas como:
- qual foi o faturamento de frete por mês
- qual foi o volume transportado
- quantas viagens foram realizadas
- qual foi o ticket médio mensal por viagem

### Valor analítico
Essa tabela é excelente para:
- tendência mensal
- visão gerencial
- dashboards executivos

---

## BLOCO 9 — Gold de Custo por Motorista

### Objetivo
Medir custo operacional por motorista por período.

### Tabela gerada
`workspace.default.gold_custo_motorista_mes`

### Fontes utilizadas
- `silver_km_rodado`
- `silver_cadastros`

### Métricas produzidas
- `km_total`
- `litros_total`
- `custo_total_combustivel`
- `custo_total_manutencao`
- `custo_total_fixo`
- `custo_total_operacional`
- `custo_por_km`
- `custo_medio_por_litro`

### Por que essa Gold foi criada
Ela permite entender:
- quais motoristas estão associados aos maiores custos
- qual o custo por km
- como evolui a eficiência operacional por motorista

### Valor analítico
Esse dataset é ideal para:
- ranking de motoristas
- comparação mensal
- análise de custo operacional

---

## BLOCO 10 — Gold de KM Rodado por Mês

### Objetivo
Consolidar uma visão mensal da frota.

### Tabela gerada
`workspace.default.gold_km_rodado_mes`

### Métricas produzidas
- `km_total`
- `litros_total`
- `custo_total_combustivel`
- `custo_total_manutencao`
- `custo_total_fixo`
- `custo_total_operacional`
- `quantidade_veiculos_ativos`
- `quantidade_motoristas_ativos`
- `km_medio_por_veiculo`
- `consumo_medio_km_por_litro`
- `custo_por_km`

### Por que essa Gold foi criada
Ela entrega uma visão consolidada da operação, útil para:
- comparar meses
- entender variação de custos
- monitorar eficiência da frota

---

# Camada de consumo

## BLOCO 11 — Validação Final das Tabelas Gold

### Objetivo
Garantir que as tabelas Gold estejam prontas para consumo.

### O que foi validado
- contagem de registros
- schema
- amostra de dados

### Por que esse bloco é importante
Antes de publicar dados para BI, é essencial validar:
- estrutura final
- consistência dos nomes
- se as colunas esperadas realmente existem

---

## BLOCO 12 — Views Analíticas para Power BI

### Objetivo
Criar uma camada semântica de consumo para o Power BI.

### Views criadas
- `workspace.default.vw_bi_kpi_frete_mes`
- `workspace.default.vw_bi_custo_motorista_mes`
- `workspace.default.vw_bi_km_rodado_mes`

### Por que usar views
Conectar o Power BI em views é uma prática melhor do que conectá-lo diretamente às tabelas físicas porque:
- reduz acoplamento
- permite trocar lógica por trás sem quebrar o relatório
- melhora governança
- simplifica a exposição dos dados

---

# Tabelas finais do projeto

## Bronze
- `workspace.default.bronze_cadastros`
- `workspace.default.bronze_frete`
- `workspace.default.bronze_km_rodado`

## Silver
- `workspace.default.silver_cadastros`
- `workspace.default.silver_frete`
- `workspace.default.silver_km_rodado`

## Gold
- `workspace.default.gold_kpi_frete_mes`
- `workspace.default.gold_custo_motorista_mes`
- `workspace.default.gold_km_rodado_mes`

## Views de consumo
- `workspace.default.vw_bi_kpi_frete_mes`
- `workspace.default.vw_bi_custo_motorista_mes`
- `workspace.default.vw_bi_km_rodado_mes`

---

# Conexão com Power BI

## Passo a passo

1. Abrir o **Power BI Desktop**
2. Clicar em **Obter Dados**
3. Procurar por **Azure Databricks**
4. Informar:
   - **Server Hostname**
   - **HTTP Path**
   - **Personal Access Token**

Essas informações são obtidas no Databricks, normalmente a partir de um **SQL Warehouse**.

## Após a conexão
Selecionar:
- catálogo: `workspace`
- schema: `default`

Depois carregar as views:
- `vw_bi_kpi_frete_mes`
- `vw_bi_custo_motorista_mes`
- `vw_bi_km_rodado_mes`

---

# Modelo dimensional para o Power BI

Até aqui, as Gold e views permitem análise imediata. Porém, para um BI profissional, o ideal é montar um **modelo estrela**.

## Proposta de esquema estrela

### Tabela fato principal 1 — `Fato Frete Mensal`
Origem:
- `vw_bi_kpi_frete_mes`

Representa:
- indicadores mensais de frete

### Tabela fato principal 2 — `Fato Custo Motorista Mensal`
Origem:
- `vw_bi_custo_motorista_mes`

Representa:
- indicadores de custo por motorista

### Tabela fato principal 3 — `Fato Frota Mensal`
Origem:
- `vw_bi_km_rodado_mes`

Representa:
- indicadores mensais de frota

### Dimensão Data
Deve ser criada no Power BI ou no próprio Databricks, contendo:
- data
- ano
- número do mês
- nome do mês
- trimestre
- ano-mês

### Dimensão Motorista
Pode ser criada a partir de `silver_cadastros` ou diretamente da Gold por motorista.

Campos principais:
- `sk_motorista`
- `motorista`

---

## Relações recomendadas

### Relação 1
`DimData[Ano]` + `DimData[NumeroMes]` → fatos mensais

Como o Power BI não cria relação composta diretamente com duas colunas com a mesma simplicidade de um banco relacional tradicional, o ideal é criar uma chave mensal.

### Chave recomendada: `ano_mes`
Formato:
```text
2024-01
2024-02
2024-03
```

Essa chave deve existir em:
- `DimData`
- `Fato Frete Mensal`
- `Fato Custo Motorista Mensal`
- `Fato Frota Mensal`

### Relação 2
`DimMotorista[sk_motorista]` → `Fato Custo Motorista Mensal[sk_motorista]`

### Cardinalidade esperada
- Dimensões → lado 1
- Fatos → lado muitos

### Direção de filtro
Preferencialmente:
- filtro unidirecional da dimensão para o fato

Isso melhora:
- desempenho
- previsibilidade dos cálculos
- clareza do modelo

---

# Como montar a estrela no Power BI

## 1. Criar a DimData

Você pode criar com DAX:

```DAX
DimData =
ADDCOLUMNS(
    CALENDAR(DATE(2018,1,1), DATE(2025,12,31)),
    "Ano", YEAR([Date]),
    "NumeroMes", MONTH([Date]),
    "NomeMes", FORMAT([Date], "MMMM"),
    "AnoMes", FORMAT([Date], "YYYY-MM"),
    "Trimestre", "T" & FORMAT([Date], "Q")
)
```

Depois crie uma tabela mensal distinta, se preferir relacionar por mês em vez de por data diária.

Exemplo:

```DAX
DimDataMensal =
SUMMARIZE(
    DimData,
    DimData[Ano],
    DimData[NumeroMes],
    DimData[AnoMes],
    DimData[NomeMes],
    DimData[Trimestre]
)
```

## 2. Criar a chave `AnoMes` nas tabelas fato

Exemplo em DAX:

```DAX
AnoMes = FORMAT(DATE([ano], [mes], 1), "YYYY-MM")
```

ou, quando a coluna de mês for `numero_mes`:

```DAX
AnoMes = FORMAT(DATE([ano], [numero_mes], 1), "YYYY-MM")
```

## 3. Criar a dimensão de motorista

Se quiser uma dimensão simples no Power BI:

```DAX
DimMotorista =
DISTINCT(
    SELECTCOLUMNS(
        'vw_bi_custo_motorista_mes',
        "sk_motorista", [sk_motorista],
        "motorista", [motorista]
    )
)
```

## 4. Criar os relacionamentos

Relacionamentos recomendados:

- `DimDataMensal[AnoMes]` → `Fato Frete Mensal[AnoMes]`
- `DimDataMensal[AnoMes]` → `Fato Custo Motorista Mensal[AnoMes]`
- `DimDataMensal[AnoMes]` → `Fato Frota Mensal[AnoMes]`
- `DimMotorista[sk_motorista]` → `Fato Custo Motorista Mensal[sk_motorista]`

---

# Medidas recomendadas no Power BI

A seguir estão medidas importantes, com explicação do porquê de cada uma.

## Medidas para Frete

### Frete Total

```DAX
Frete Total = SUM('Fato Frete Mensal'[valor_total_frete_liquido])
```

**Por que existe:**  
É a principal medida de faturamento logístico.

---

### Total de Viagens

```DAX
Total de Viagens = SUM('Fato Frete Mensal'[quantidade_viagens])
```

**Por que existe:**  
Permite medir volume operacional.

---

### Peso Total KG

```DAX
Peso Total KG = SUM('Fato Frete Mensal'[peso_total_kg])
```

**Por que existe:**  
Mostra a carga transportada em massa real.

---

### Ticket Médio por Viagem

```DAX
Ticket Médio por Viagem =
DIVIDE([Frete Total], [Total de Viagens], 0)
```

**Por que existe:**  
Ajuda a entender o valor médio gerado por viagem.

---

### Valor Total da Mercadoria

```DAX
Valor Total da Mercadoria = SUM('Fato Frete Mensal'[valor_total_mercadoria])
```

**Por que existe:**  
Ajuda a comparar valor transportado versus receita de frete.

---

## Medidas para Custo por Motorista

### Custo Operacional Total

```DAX
Custo Operacional Total = SUM('Fato Custo Motorista Mensal'[custo_total_operacional])
```

**Por que existe:**  
É a base para qualquer análise de custo logístico por motorista.

---

### KM Total Motorista

```DAX
KM Total Motorista = SUM('Fato Custo Motorista Mensal'[km_total])
```

**Por que existe:**  
Mede produtividade operacional.

---

### Custo por KM

```DAX
Custo por KM =
DIVIDE([Custo Operacional Total], [KM Total Motorista], 0)
```

**Por que existe:**  
É uma das métricas mais importantes da logística, porque traduz custo operacional em eficiência de deslocamento.

---

### Litros Totais

```DAX
Litros Totais = SUM('Fato Custo Motorista Mensal'[litros_total])
```

**Por que existe:**  
Permite acompanhar consumo de combustível.

---

### Custo Médio por Litro

```DAX
Custo Médio por Litro =
DIVIDE(
    SUM('Fato Custo Motorista Mensal'[custo_total_combustivel]),
    [Litros Totais],
    0
)
```

**Por que existe:**  
Ajuda a medir variação de custo do combustível e eficiência da operação.

---

## Medidas para Frota

### KM Total Frota

```DAX
KM Total Frota = SUM('Fato Frota Mensal'[km_total])
```

**Por que existe:**  
É a base do volume operacional da frota.

---

### Custo Total Frota

```DAX
Custo Total Frota = SUM('Fato Frota Mensal'[custo_total_operacional])
```

**Por que existe:**  
Mostra o custo consolidado da operação.

---

### Custo por KM Frota

```DAX
Custo por KM Frota =
DIVIDE([Custo Total Frota], [KM Total Frota], 0)
```

**Por que existe:**  
Resume a eficiência econômica da frota inteira.

---

### Veículos Ativos

```DAX
Veículos Ativos = SUM('Fato Frota Mensal'[quantidade_veiculos_ativos])
```

**Por que existe:**  
Permite comparar o tamanho da frota operante ao longo do tempo.

> Observação: como essa métrica já vem agregada por mês na Gold, a soma funciona no contexto mensal. Em outros contextos, pode ser mais adequado usar agregação diferente dependendo do visual.

---

### Motoristas Ativos

```DAX
Motoristas Ativos = SUM('Fato Frota Mensal'[quantidade_motoristas_ativos])
```

**Por que existe:**  
Ajuda a medir capacidade operacional do período.

---

### Consumo Médio KM por Litro

```DAX
Consumo Médio KM por Litro =
DIVIDE([KM Total Frota], SUM('Fato Frota Mensal'[litros_total]), 0)
```

**Por que existe:**  
É um indicador clássico de eficiência de combustível.

---

# Sugestão de páginas no Power BI

## Página 1 — Visão Executiva
Indicadores:
- Frete Total
- Total de Viagens
- Custo Total Frota
- Custo por KM Frota

Visuais:
- linha de tendência por mês
- cartões
- colunas por mês

## Página 2 — Desempenho de Frete
Indicadores:
- Frete Total
- Peso Total KG
- Ticket Médio por Viagem

Visuais:
- linha de faturamento
- colunas de viagens
- combinação de receita e peso

## Página 3 — Custo por Motorista
Indicadores:
- Custo Operacional Total
- KM Total Motorista
- Custo por KM

Visuais:
- ranking de motoristas
- tabela detalhada
- comparação por período

## Página 4 — Eficiência da Frota
Indicadores:
- KM Total Frota
- Consumo Médio KM por Litro
- Veículos Ativos

Visuais:
- linha de eficiência
- barras de custo
- cartões operacionais

---

# Boas práticas aplicadas no projeto

## 1. Separação por camadas
A arquitetura Medallion facilita:
- governança
- rastreabilidade
- manutenção
- evolução incremental

## 2. Bronze preservando o dado original
Isso ajuda auditoria e permite reprocessamento.

## 3. Silver padronizada
A Silver foi criada para isolar os problemas de qualidade e entregar tabelas confiáveis.

## 4. Gold orientada ao negócio
As Gold não foram construídas só para armazenar dados, mas para responder perguntas reais do negócio.

## 5. Camada de consumo com views
Isso desacopla o BI da estrutura física.

## 6. Modelo estrela no Power BI
Isso melhora:
- desempenho
- usabilidade
- clareza analítica
- manutenção de medidas

---

# Próximos passos recomendados

Para evoluir este projeto para um nível ainda mais profissional, os próximos incrementos seriam:

- carga incremental com `MERGE INTO`
- particionamento de tabelas
- orquestração com Workflows do Databricks
- testes de qualidade de dados
- criação de calendário e dimensões diretamente no Databricks
- publicação automatizada para consumo analítico

---

# Resumo executivo

Este projeto demonstrou a construção de um pipeline de dados completo em Databricks para uma empresa logística fictícia, utilizando a arquitetura Medallion, tratamento multi-formato, modelagem analítica e disponibilização dos dados para Power BI com proposta de esquema estrela.

Além da engenharia de dados, o projeto incorporou uma visão de BI, incluindo:
- modelagem dimensional
- relações
- medidas
- estrutura de dashboards

Por se tratar de **dados de teste**, o objetivo principal foi demonstrar competências técnicas de forma clara, profissional e aplicável ao mercado.

