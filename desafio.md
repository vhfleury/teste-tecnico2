# Teste Técnico — Engenheiro de Dados (Python)

Olá! Seja bem-vindo(a) ao nosso processo seletivo. Este teste foi elaborado para avaliarmos seu conhecimento técnico em engenharia de dados, sua capacidade de organização, modelagem de dados, arquitetura de pipelines e boas práticas de desenvolvimento.

Leia o documento com atenção antes de começar. Boa sorte!

---

## Cenário

Você foi contratado(a) como engenheiro(a) de dados de uma **empresa de logística** que opera uma frota de caminhões em todo o Brasil. A empresa coleta dados de **rastreamento GPS em tempo real**, controla **cercas geoespaciais** (centros de distribuição, pedágios, postos e clientes) e gerencia **viagens, veículos e motoristas**.

Hoje, esses dados estão espalhados em diferentes formatos e sistemas, sem tratamento ou consolidação. Sua missão é construir um **pipeline de dados** que transforme esses dados brutos em informação confiável para o time de operações.

Os dados brutos estão disponíveis no diretório [`data/`](./data/) e representam **5 fontes distintas**:

| Fonte | Formato | Registros | Descrição |
|---|---|---|---|
| `veiculos/veiculos.csv` | CSV | ~150 | Cadastro da frota |
| `motoristas/motoristas.json` | JSON | ~120 | Cadastro de motoristas |
| `geocercas/geocercas.geojson` | GeoJSON | ~38 | Cercas geoespaciais (CDs, pedágios, postos, clientes) |
| `viagens/viagens.csv` | CSV | ~3.000 | Registro de viagens realizadas |
| `rastreamento/posicoes.parquet` | Parquet | ~56.000 | Posições GPS dos veículos durante as viagens |

> ⚠️ **Atenção:** os dados contêm **inconsistências propositais** (duplicatas, valores nulos, coordenadas inválidas, registros órfãos, velocidades absurdas, etc.). O tratamento dessas inconsistências faz parte da avaliação.

---

## O que deve ser feito

Construir um **pipeline ETL/ELT** que:

### 1. Extração (E)
- Leia os dados brutos das 5 fontes em seus respectivos formatos (CSV, JSON, GeoJSON, Parquet).

### 2. Transformação (T)
As seguintes transformações são **obrigatórias**:

- **Limpeza e qualidade de dados:**
  - Tratar valores nulos, duplicados e inconsistentes
  - Validar integridade referencial entre as tabelas (ex: viagens com `motorista_id` ou `veiculo_id` inexistente)
  - Filtrar ou tratar coordenadas GPS inválidas (lat/lon zeradas, fora do Brasil)
  - Remover velocidades absurdas (negativas ou fisicamente impossíveis)
  - Padronizar formatos de datas, strings e campos categóricos

- **Enriquecimento geoespacial:**
  - Para cada posição GPS do rastreamento, determinar se o veículo estava **dentro de alguma geocerca** (point-in-polygon)
  - Classificar cada posição como: `em_geocerca` (identificando qual) ou `em_rota`
  - Detectar **eventos de entrada e saída** de geocercas ao longo de cada viagem

- **Modelagem e agregações:**
  - Criar uma tabela consolidada de **viagens enriquecidas** (com dados do veículo, motorista, geocercas de origem/destino e métricas da viagem)
  - Gerar as seguintes métricas agregadas:
    - **Viagens por mês e por status** (concluída, cancelada, atrasada)
    - **Tempo médio de viagem** por rota (origem → destino)
    - **Velocidade média** por viagem
    - **Taxa de atraso** (viagens atrasadas / total) por mês
    - **Top 10 motoristas** por número de viagens concluídas
    - **Utilização da frota** (veículos ativos com viagens / total de veículos ativos) por mês
    - **Tempo médio parado em geocercas** (por tipo: CD, pedágio, posto, cliente)

### 3. Carga (L)
- Persistir os dados transformados e as métricas em um **formato estruturado** (Parquet, Delta Lake, PostgreSQL, Elasticsearch ou outro de sua escolha).
- Organizar a saída em **camadas** (ex: `raw` → `staging` → `trusted/analytics`).

---

## Stack Obrigatória

| Camada | Tecnologia |
|---|---|
| Linguagem | **Python 3.10+** |
| Processamento | **Apache Spark (PySpark)** |
| Containerização | **Docker / Docker Compose** |

### Livre escolha

- **Formato de saída:** Parquet, Delta Lake, PostgreSQL, Elasticsearch, etc.
- **Orquestração:** Airflow, Prefect, Dagster, scripts agendados, Makefile, etc.
- **Geoespacial:** Sedona (GeoSpark), GeoPandas, Shapely, H3, ou outra lib de sua preferência.

### Opcional (diferencial)

- Orquestrador com DAGs (Airflow, Prefect, Dagster)
- Apache Sedona ou outra lib geoespacial integrada ao Spark
- Delta Lake para versionamento de dados
- Testes automatizados (pytest, Great Expectations, Soda, etc.)
- Data quality checks integrados ao pipeline
- Logging estruturado
- CI com GitHub Actions (lint, testes)

---

## Requisitos Técnicos

- O projeto deve rodar em **containers Docker** (recomendado `docker-compose` para orquestrar todos os serviços).
- O projeto deve subir e executar o pipeline com **um único comando** (ex: `docker-compose up`).
- O processamento principal deve ser feito com **PySpark** (não apenas Pandas).
- O pipeline deve ser **idempotente** — executá-lo mais de uma vez não deve gerar duplicação nos dados de saída.
- Variáveis de configuração devem estar em variáveis de ambiente ou arquivos de configuração, nunca hardcoded no código.

---

## Como participar

1. Faça um **fork** deste repositório.
2. Desenvolva sua solução no fork.
3. Ao finalizar, envie o **link do seu repositório público** conforme as instruções de entrega abaixo.

---

## O que será avaliado

1. **Commits contínuos e explicados**
   Queremos ver a evolução do seu raciocínio ao longo do desenvolvimento. Commits pequenos, frequentes e com mensagens claras são esperados.

2. **Arquitetura do pipeline**
   Organização das etapas (extração, transformação, carga), separação de camadas de dados, escolhas arquiteturais coerentes e justificáveis.

3. **Processamento geoespacial**
   Capacidade de trabalhar com dados de coordenadas, geocercas (point-in-polygon) e detecção de eventos de entrada/saída. Escolha e uso adequado de bibliotecas geoespaciais.

4. **Modelagem de dados**
   Estrutura dos dados de saída, escolha de formatos de armazenamento, particionamento, definição de schemas e clareza na organização dos datasets.

5. **Qualidade e organização do código**
   Código limpo, legível, nomes significativos, padrões consistentes, uso adequado do PySpark e boas práticas Python (tipagem, docstrings, linting).

6. **Tratamento de dados sujos**
   Identificação e tratamento correto das inconsistências nos dados de entrada (nulos, duplicatas, órfãos, coordenadas inválidas, valores absurdos).

7. **Resiliência e robustez**
   Tratamento de falhas, idempotência, logging e capacidade do pipeline de lidar com cenários inesperados.

8. **README da solução**
   Ao desenvolver, atualize este README (ou crie um novo) com:
   - Descrição da sua solução e decisões técnicas
   - Arquitetura do pipeline (preferencialmente com um diagrama)
   - Tecnologias utilizadas e justificativas
   - Pré-requisitos
   - Instruções claras para rodar o projeto
   - Variáveis de ambiente necessárias (se houver)
   - Estrutura de pastas
   - O que você faria diferente com mais tempo

---

## Critérios de Exclusão

O candidato será **automaticamente desclassificado** caso:

- O container **não inicialize** ou apresente **erro** ao subir a aplicação.
- O pipeline **não execute** ou não produza os dados de saída esperados.
- Forem detectados **poucos commits** ou **um único commit**, indicando uso abusivo de IA ou falta de desenvolvimento incremental.
- **PySpark não for utilizado** como engine de processamento principal.
- O **enriquecimento geoespacial** (point-in-polygon) não for implementado.
- As **transformações obrigatórias** não forem implementadas.

> O uso de ferramentas de IA como apoio é aceitável, mas queremos ver **seu processo de desenvolvimento**, suas decisões e sua evolução ao longo do projeto.

---

## Entregável

- **Link público do repositório no GitHub** contendo o projeto completo (fork deste repositório).
- **Prazo final**: Será informado no contato com o candidato.

### Envio

O link do repositório deve ser enviado por e-mail para os endereços informados no contato com o candidato.

**Assunto sugerido**: `Teste Técnico — Engenheiro de Dados — [Seu Nome Completo]`

No corpo do e-mail, inclua:

- Seu nome completo
- Link público do repositório no GitHub
- Breve descrição (1 a 2 parágrafos) sobre a arquitetura do pipeline e as decisões técnicas principais

---

## Estrutura do repositório

```
.
├── README.md                          # Este arquivo
├── data/
│   ├── veiculos/
│   │   └── veiculos.csv               # Cadastro da frota (~150 registros)
│   ├── motoristas/
│   │   └── motoristas.json            # Cadastro de motoristas (~120 registros)
│   ├── geocercas/
│   │   └── geocercas.geojson          # Cercas geoespaciais (~38 polígonos)
│   ├── viagens/
│   │   └── viagens.csv               # Registro de viagens (~3.000 registros)
│   └── rastreamento/
│       └── posicoes.parquet           # Posições GPS (~56.000 registros)
└── docs/
    └── dados.md                       # Dicionário de dados
```

---

## Dúvidas

Em caso de dúvidas sobre o teste, entre em contato com as pessoas que estão conduzindo o processo seletivo.

Bom desenvolvimento!
