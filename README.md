# Plataforma de Inteligência Logística e Gestão de Cadeia de Mantimentos em Tempo Real
## Sistema Operacional Orientado a Eventos e Agentes de IA
### Especificação Técnica de Referência e Blueprint de Arquitetura Corporativa

---

## 🏛️ 1. DIAGRAMA DE ARQUITETURA E FLUXO FIM A FIM (SUPPLY CHAIN)

Esta plataforma foi projetada sob o paradigma **Offline-First**, combinando processamento de eventos por vazão ultra-alta na nuvem e processamento de regras complexas em tempo real com orquestração de tomada de decisão baseada em sistemas multi-agentes de IA. O objetivo é mitigar gargalos operacionais globais, tais como desconexão de frotas e sensoriamento IoT instável em alto-mar e galpões remotos.

### 🗺️ Arquitetura do Sistema Fim a Fim (ASCII)

```text
+=============================================================================================================================+
|                                                    1. CAMADA DE APRESENTAÇÃO                                                |
|   +----------------------------------+       +------------------------------------+       +-----------------------------+   |
|   | Dashboard Web (ECharts + React)  |◄=====&gt;|    Servidor de WebSockets (WS)     |◄=====&gt;|  Notificações push (SSE)    |   |
|   |  - Mapas de calor de frotas      |  WS   |  - Envio de alertas de SLA ativos  |  gRPC |  - Alertas instantâneos     |   |
|   |  - Gráficos de Gantt de entregas |       |  - Posição da frota sub-segundo    |       |  - Desvios aduaneiros       |   |
|   +----------------------------------+       +------------------------------------+       +-----------------------------+   |
+=============================================================================================================================+
                                                            ▲
                                                            │ Atualizações em Tempo Real
                                                            ▼
+=============================================================================================================================+
|                                            2. ORQUESTRAÇÃO DE NEGÓCIO, IA E AGENTES                                         |
|   +----------------------------------+       +------------------------------------+       +-----------------------------+   |
|   |  vLLM / Ollama (Infr. Local)     |◄=====&gt;|   Agent Starter Kit (Multi-Agent)  |◄=====&gt;|  Dubbo 3 / bRPC microservices |  |
|   |  - llama3.1:8b & qwen2.5-coder    | API   |  - Negociação autônoma de frete    | Triple|  - Cotação de frete         |   |
|   |  - Execução offline de LLM       |       |  - Auditoria automatizada de faturas|      |  - Motor de alocação        |   |
|   +----------------------------------+       +------------------------------------+       +-----------------------------+   |
+=============================================================================================================================+
                                                            ▲                                 │
                                                            │ Consumo e Alertas               │ Autorização (ABAC/RBAC)
                                                            ▼                                 ▼
+=============================================================================================================================+
|                                           3. PROCESSAMENTO DE EVENTOS & COMPLEX STREAMING                                    |
|   +----------------------------------+       +------------------------------------+       +-----------------------------+   |
|   |   Apache RocketMQ (Barramento)   |======&gt;|      Apache Flink (Stream CEP)     |======&gt;|    Apache Casbin Middleware  |   |
|   |  - Ingestão de telemetria IoT    | TCP   |  - Regras Complexas de Geofencing  |  gRPC |  - Segurança zero-trust     |   |
|   |  - Mensagens transacionais       |       |  - RocksDB State Backend (Exactly) |       |  - Controle multi-tenant    |   |
|   +----------------------------------+       +------------------------------------+       +-----------------------------+   |
+=============================================================================================================================+
                                                            ▲
                                                            │ Sincronização Semicontínua
                                                            ▼
+=============================================================================================================================+
|                                                4. CAMADA DE PERSISTÊNCIA GLOBAL                                             |
|   +--------------------------------------------------+       +----------------------------------------------------------+   |
|   |       Apache CouchDB (Central Cloud Hub)         |◄=====&gt;|          Apache ShardingSphere DB Proxy                  |   |
|   |  - Banco NoSQL orientado a revisões (MVCC)       | Replic|  - Sharding horizontal por região geográfica (PostgreSQL)|   |
|   |  - Sincronização mestre-mestre com frotas        |       |  - Criptografia transparente em repouso (Column Encryption)|  |
|   +--------------------------------------------------+       +----------------------------------------------------------+   |
+=============================================================================================================================+
                                                            ▲
                                                            │ Sincronização Bidirecional P2P (HTTP/JSON)
                                                            ▼
+=============================================================================================================================+
|                                               5. BORDA, FROTA E INTERFACE OFFLINE                                           |
|   +--------------------------------------------------+       +----------------------------------------------------------+   |
|   |       PouchDB Client (Local no Tablet/Coletor)    |◄=====&gt;|                Apache TVM Edge AI Engine                 |   |
|   |  - Armazenamento em IndexedDB / SQLite (Offline)  | Local |  - Redes neurais enxutas compiladas para hardware        |   |
|   |  - Replicação de dados em reconectividade        |       |  - Visão computacional de avarias em containers/cargas   |   |
|   +--------------------------------------------------+       +----------------------------------------------------------+   |
+=============================================================================================================================+
```

---

### 📊 Tabela Mestra de Racionalização Arquitetural

| Componente Logístico | Ferramenta Escolhida | Protocolo Utilizado | Garantia de SLA | Racional Técnico & Justificativa |
| :--- | :--- | :--- | :--- | :--- |
| **Barramento de Mensagens** | Apache RocketMQ (v5.3) | TCP / gRPC / custom RocketMQ binary | **99.999% disponibilidade**, Latência de ingestão **< 5ms** | Chosen for its built-in transactional message coordination, high performance under massive concurrent queues, and robust dead-letter queue (DLQ) support compared to Kafka. |
| **Processamento de Streams & CEP** | Apache Flink (v1.19) | Custom TCP streams / Flink Network | Latência de detecção de anomalias **< 10ms** | Stateful stream processing with RocksDB state backend allows exactly-once aggregations, sliding-window calculations, and complex geofencing validations. |
| **Comunicação Inter-Serviços** | Apache Dubbo 3 / bRPC | Triple Protocol (HTTP/2 with Protobuf) | Latência de RPC **< 1ms**, 100k+ TPS | Triple Protocol ensures high-performance binary payload serialization and full gRPC compatibility across polyglot microservices. |
| **Banco Offline-First na Borda** | PouchDB (dispositivos) & Apache CouchDB (nuvem) | HTTP / JSON Replication Protocol | Replicação eventual garantida, gravação local com **latência zero** | Uses Multi-Version Concurrency Control (MVCC) to let drivers write records locally during disconnection, with automatic conflict resolution upon reconnection. |
| **Particionamento de Dados** | Apache ShardingSphere (v5.4) | PostgreSQL Wire Protocol (TCP) | **99.99% consistência**, Escrita distribuída sem travas globais | Transparent SQL routing, column-level security encryption, and horizontal table partitioning by geographical region without changing backend code. |
| **Orquestração de Pipelines Batch** | Apache Airflow (v2.9) | HTTP / Celery Executors (AMQP) | Conclusão de DAGs de reconciliação em **< 1h** | Scheduled automated financial reconciliations, customs duty reporting, and vector database pre-indexing schedules. |
| **Compilação Edge AI** | Apache TVM | Dynamic C++ Compiled Shared Library | Tempo de inferência local **< 15ms** on ARM targets | Compiles and optimizes neural networks for specialized edge processors (ARM, NPU) directly on container hardware to detect damage and cargo faults without internet. |
| **Agentes de IA Locais** | Agent Starter Kit + Ollama / vLLM | HTTP / REST API (OpenAI compatible) | Tempo de resposta para auditoria de nota fiscal **< 1.5s** | Private LLM orchestration directly in the regional hub. Performs zero-leak transport invoice auditing and autonomous carrier negotiations. |
| **Autorização de Acesso** | Apache Casbin | Native Library / Embedded In-Memory Filter | Tempo de decisão de acesso **< 0.1ms** | Flexible Policy Enforcer (PERM model) embedded in the RPC interceptor layer for high-throughput tenant security isolation. |
| **Observabilidade & Tracing** | Apache SkyWalking | gRPC / HTTP (sw8 tracing headers) | Overhead de telemetria **< 1%** da CPU | Implements end-to-end distributed transaction tracing, network topology discovery, and eBPF-based profiling across polyglot microservice boundaries. |
| **Painel de Controle Operacional** | Apache ECharts | WebSockets (WS) | Taxa de atualização fluida de **60 FPS** | High-performance Canvas/SVG vector graphics engine rendering thousands of real-time fleet coordinates without UI freezing. |

---

## ❄️ 2. CASO DE USO PRÁTICO: "Gestão Inteligente de Cargas Críticas e Cadeia de Frio"

Este cenário descreve o ciclo de vida completo de ponta a ponta dos dados gerados por sensores instalados em um contêiner refrigerado transportando insumos hospitalares (vacinas termolábeis) e alimentos perecíveis.

### Fluxo de Trabalho Passo a Passo

```text
[Sensores IoT] ──► (PouchDB Offline Local)
                            │
                  (Sinal de Rede Restabelecido)
                            │
                            ▼
              (Recepção no CouchDB Hub Central)
                            │
                            ▼
              (Apache RocketMQ: Ingestão de Alta Vazão)
                            │
                            ▼
               (Apache Flink: Processador CEP) ──► [Alerta de Desvio Detectado!]
                            │
                            ▼
                (Agent Starter Kit via vLLM) ──► [Pesquisa Estoque & bRPC Re-Roteamento]
                            │
                            ▼
               (Casbin Autoriza & ECharts Renderiza Novo Plano)
```

#### Passo 1: Coleta e Borda (Edge Tier)
- **Ação:** O caminhão transportando vacinas passa por uma área montanhosa sem qualquer cobertura de sinal de celular ou satélite.
- **Gravação Local:** Os sensores IoT do contêiner registram leituras de temperatura (ex. +4.2°C, limite de segurança: +2.0°C a +8.0°C) e coordenadas de GPS. O aplicativo web no tablet do motorista grava os eventos instantaneamente na base local **PouchDB**.
- **Persistência Sem Rede:** Como a conexão está inativa, o PouchDB armazena os dados localmente de forma resiliente usando o IndexedDB do navegador. O motorista pode prosseguir viagem sabendo que nenhum pacote de telemetria foi perdido.

#### Passo 2: Sincronização e Ingestão (Gateway Tier)
- **Ação:** Ao se aproximar de um hub rodoviário ou ponto com sinal 4G/5G, o tablet se reconecta à rede.
- **Replicação Ativa:** O motor de replicação bidirecional do PouchDB detecta a conexão e inicia um sincronismo HTTP contra a base centralizada **Apache CouchDB** rodando no datacenter local ou nuvem pública.
- **Pipelining de Eventos:** O Gateway de Borda consome esses eventos sincronizados do CouchDB e os injeta imediatamente no barramento de eventos **Apache RocketMQ** sob o tópico de alta vazão `coldchain_telemetry`. O RocketMQ garante entrega com latência de milissegundos e suporte para recuperação em caso de falha de brokers.

#### Passo 3: Análise em Tempo Real & CEP (Processing Tier)
- **Ação:** O framework **Apache Flink** consome as mensagens em tempo real vindas do tópico `coldchain_telemetry`.
- **Complex Event Processing (CEP):** Flink executa uma janela temporal deslizante de 5 minutos avaliando a temperatura do contêiner. Se a temperatura subir acima de +8.0°C por um período contínuo de 5 minutos, ou se o caminhão desviar em mais de 500 metros do polígono de geofence de rota traçado, o Flink emite instantaneamente um evento complexo de anomalia crítica: `COLD_CHAIN_BREACH_ALERT` para o RocketMQ.

#### Passo 4: Ação Autônoma do Agente de IA (Intelligence Tier)
- **Ação:** O sistema multi-agentes construído com o **Agent Starter Kit** consome o alerta crítico do RocketMQ.
- **Processamento Privado:** Rodando localmente através de servidores **vLLM / Ollama** (para proteção de dados de faturamento e rotas privadas sem uso de APIs externas), o Agente de Decisão Logística assume o caso.
- **Consulta de Estoque via RPC:** O agente invoca, via **Apache Dubbo 3** (usando protocolo Triple de ultra-baixa latência), o microserviço de Inventário (`InventoryService/getNearestSpares`) e o motor de alocação de rotas (`FleetAllocationService/reallocateVehicle`) para descobrir se o hub logístico mais próximo possui um contêiner refrigerado sobressalente disponível.
- **Planejamento:** O agente de IA analisa os tempos de trânsito, as restrições de custos e redige uma proposta de desvio de rota e parada de emergência no hub rodoviário mais próximo para transferir a carga vacinal antes que haja perda total do lote.

#### Passo 5: Segurança, Governança e Visibilidade (Security & UX Tier)
- **Ação:** O novo plano de contingência gerado pelo agente precisa de aprovação operacional ou execução sistêmica.
- **Intercepção de Segurança:** O microserviço de alocação de frota recebe a requisição de desvio de rota. O interceptador de segurança **Apache Casbin** avalia se o usuário operador ou o próprio ID do Agente de IA têm permissão (`role = logistic_agent`) para alterar rotas sob o domínio tenant específico (`domain = hospital_supplies`).
- **Exibição Gráfica:** Após a aprovação rápida do Casbin, a nova rota modificada é persistida no banco distribuído por meio do **Apache ShardingSphere** (garantindo transações fortemente consistentes XA para estoque e roteamento).
- **Painel em Tempo Real:** A torre de controle centralizada recebe o alerta e a rota atualizada via WebSocket. O componente **Apache ECharts** renderiza dinamicamente o mapa de calor com a nova localização do caminhão, desenha o desvio em tempo real e atualiza as métricas de SLA na tela operacional de controle em 60 FPS, garantindo transparência operacional completa.

---

## ⚙️ 3. RESILIÊNCIA LOGÍSTICA E TRADEOFFS TÉCNICOS

### A. Estratégia de Sincronização e Reconciliação do CouchDB para Falhas de Rede Prolongadas
No cenário marítimo de transporte internacional ou em estradas desprovidas de cobertura celular por dias, a replicação offline-first torna-se o pilar central. A plataforma gerencia este desafio através do modelo de dados baseado em revisões do CouchDB:

1. **Multi-Version Concurrency Control (MVCC):** Cada gravação local gera uma nova folha no grafo de revisões do documento (ex: `1-rev1`, `2-rev2`). Quando o nó da embarcação está desconectado, ele continua gerando alterações locais.
2. **Deterministic Conflict Resolution (Resolução Determinística de Conflitos):** Ao restabelecer a conexão, ambas as bases (Edge e Cloud) sincronizam suas árvores de revisão. Havendo edições concorrentes no mesmo documento, o CouchDB não impede a sincronização; em vez disso, ele marca o documento com a flag `_conflicts` e elege deterministicamente uma revisão vencedora (usando hash lexicográfico de revisão).
3. **Algoritmo de Reconciliação de Negócio (Reconciliation Engine):** Para dados operacionais de telemetria, aplicamos a estratégia **Last-Write-Wins (LWW)** baseada no timestamp do sensor. Para relatórios aduaneiros e faturas de transporte, um trabalhador assíncrono (Airflow) executa um merge semântico de conflitos, garantindo que logs de auditoria concorrentes criados pelo motorista e pelo agente aduaneiro sejam consolidados em um histórico linear, preservando todas as assinaturas criptográficas dos eventos.

### B. Consistência de Dados vs Latência: O Dilema de CAP Resolvido
Diferentes domínios operacionais demandam diferentes garantias transacionais na nossa plataforma:

1. **Consistência Forte (Strong Consistency / Acid / 2PC via ShardingSphere):**
   - *Escopo:* Gestão de inventário global, faturamento de fretes, atribuição financeira de motoristas e movimentação de estoque.
   - *Mecanismo:* O Apache ShardingSphere atua como proxy JDBC. Ao executar transações que afetam o estoque de múltiplos hubs shardados geograficamente, ele habilita o modo de transação **XA (Two-Phase Commit)**. Isso garante que a reserva de carga em um hub e a baixa no hub de origem ocorram de forma atômica, prevenindo duplicidade de alocação de cargas sob alto estresse concorrente, sacrificando levemente a latência (tempo de resposta de ~50-100ms devido ao lock distribuído).
2. **Consistência Eventual (Eventual Consistency / Alta Ingestão / No Lock):**
   - *Escopo:* Rastreamento de localização geográfica em tempo real, monitoramento contínuo de temperatura e telemetria de sensores de pneus/motores.
   - *Mecanismo:* Os dados de telemetria fluem continuamente via RocketMQ para o Flink e são persistidos no CouchDB e indexados no Solr/Elasticsearch. Não há locks transacionais. Se um operador consultar a posição de um caminhão, pode ver uma coordenada atrasada em frações de segundo (Consistência Eventual), o que é aceitável em troca de suportar milhões de leituras e gravações por segundo com latência de processamento sub-segundo.

---

## 🛠️ 4. MANUAL DE INSTALAÇÃO, DEPLOY E TESTE

Para implantar e validar este ecossistema corporativo completo em ambiente local ou de staging, siga as instruções abaixo:

### Pré-requisitos
- Docker Engine >= v20.10 e Docker Compose >= v2.10 instalados.
- Python >= 3.10 instalado para execução do agente local de IA.
- Java JDK 17+ instalado para compilação opcional do job Flink ou execução do interceptador Java.

### Passo 1: Inicializando a Infraestrutura de Suporte
Navegue até a pasta do projeto e execute o comando docker-compose para subir os serviços do RocketMQ, Flink, CouchDB, Qdrant Vector DB, Ollama, e o painel de suporte:
```bash
cd intelligent-logistics-platform
docker compose up -d
```

### Passo 2: Verificação de Saúde da Infraestrutura
Certifique-se de que todos os containers estão em execução e saudáveis:
```bash
docker compose ps
```
Você pode verificar as interfaces web criadas:
* **Apache Flink Web UI:** `http://localhost:8081`
* **CouchDB Admin Portal (Fauxton):** `http://localhost:5984/_utils` (Login: `admin` / `secretpassword`)

### Passo 3: Instalando Dependências do Agente de IA Local
Abra um terminal, acesse o diretório do agente e instale os pacotes python requeridos:
```bash
cd ai-agent
pip install -r requirements.txt
```
*(Nota: Certifique-se de ter o Ollama local configurado ou vLLM rodando e aponte a URL base adequadamente no script do agente).*

---
*Assinado e Homologado:*
**Arquiteto Principal de Sistemas Corporativos, CTO e Especialista em Supply Chain.**
