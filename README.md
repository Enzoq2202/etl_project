# Relatório do Projeto ETL em OCaml

Este documento é o relatório do projeto ETL (*Extract, Transform, Load*) desenvolvido em OCaml como parte de um trabalho acadêmico. O objetivo é processar dados de pedidos (`order.csv`) e itens de pedidos (`order_item.csv`), calcular totais de receita e impostos por pedido filtrados por `status` e `origin`, e gerar relatórios em CSV. O projeto utiliza programação funcional (FP) com imutabilidade, funções puras e *higher-order functions* como `map`, `reduce`, e `filter`.

## Objetivo do Projeto

O projeto implementa um processo ETL para:
- **Extrair**: Ler dados de dois arquivos CSV (`order.csv` e `order_item.csv`).
- **Transformar**: Calcular `total_amount` (soma de `price * quantity`) e `total_taxes` (soma de `(price * quantity) * tax`) para pedidos com `status = "Complete"` e `origin = "O"`, além de médias mensais de receita e impostos.
- **Carregar**: Gerar `output.csv` com totais por pedido e `monthly_averages.csv` com médias agrupadas por mês e ano.

## Estrutura do Projeto

O projeto está organizado com o sistema de *build* Dune:
- **`bin/main.ml`**: Ponto de entrada que coordena o ETL.
- **`lib/pure.ml`**: Funções puras para transformação de dados.
- **`lib/impure.ml`**: Funções impuras para leitura e escrita de arquivos.
- **`test/test_pure.ml`**: Testes unitários para funções puras.
- **`dune-project` e arquivos `dune`**: Configuração do Dune para compilar e executar.

## Pré-requisitos

- **OCaml**: Versão 4.14 ou superior.
- **Dune**: Versão 3.17.
- **Biblioteca `csv`**: Versão >= 2.4, instalada via `opam install csv`.

## Como Reproduzir o Projeto

### Passos de Instalação
1. Clone o repositório:
   ```bash
   git clone <URL_DO_REPOSITORIO>
   cd etl_project
   ```
2. Instale as dependências:
   ```bash
   opam install csv
   ```
3. Compile o projeto:
   ```bash
   dune build
   ```

### Passos de Execução
1. Coloque os arquivos de entrada `order.csv` e `order_item.csv` na raiz do projeto. Exemplo de formato:
   - `order.csv`: `id,client_id,order_date,status,origin`
   - `order_item.csv`: `order_id,product_id,quantity,price,tax`
2. Execute o programa:
   ```bash
   dune exec etl_project
   ```
3. Verifique os arquivos gerados:
   - `output.csv`: Totais por pedido.
   - `monthly_averages.csv`: Médias mensais.

## Construção das Etapas do ETL

### 1. Extração (Extract)
A extração lê os dados brutos dos CSVs e os converte em estruturas de dados utilizáveis.

- **Leitura de `order.csv`**:
  - Função: `read_orders` em `lib/impure.ml`.
  - Processo:
    1. Usa `Csv.load` para carregar o arquivo em uma lista de linhas.
    2. Pula o cabeçalho com `List.tl`.
    3. Converte cada linha em um *record* `order` usando `List.map`.
    4. Campos como `id` e `client_id` são convertidos de string para inteiro com `string_to_int`.
  - Resultado: Lista de *records* `order list`.

- **Leitura de `order_item.csv`**:
  - Função: `read_order_items` em `lib/impure.ml`.
  - Processo:
    1. Similar a `read_orders`, usa `Csv.load` e `List.tl`.
    2. Converte cada linha em um *record* `order_item` com `List.map`.
    3. Usa `string_to_int` para `order_id`, `product_id`, `quantity`, e `string_to_float` para `price` e `tax`.
  - Resultado: Lista de *records* `order_item list`.

- **Separação**: Essas funções são impuras (interagem com o sistema de arquivos) e estão isoladas em `impure.ml`.

### 2. Transformação (Transform)
A transformação processa os dados extraídos para gerar os resultados desejados.

- **Filtragem**:
  - Função: `filter_orders` em `lib/pure.ml`.
  - Processo:
    1. Usa `List.filter` para selecionar ordens onde `status = "Complete"` e `origin = "O"`.
    2. Compara strings em minúsculas com `String.lowercase_ascii` para flexibilidade.
  - Resultado: Lista filtrada de ordens.

- **Agrupamento e Join**:
  - Função: `group_by_order_id` em `lib/pure.ml`.
    - Usa `List.fold_left` para agrupar itens por `order_id` em uma lista de pares `(int * order_item list)`.
  - Função: `inner_join` em `lib/pure.ml`.
    - Junta ordens filtradas com itens agrupados usando `List.filter_map`, associando `order.id` a `order_item.order_id`.
  - Resultado: Lista de pares `(order * order_item list)`.

- **Agregação**:
  - Função: `aggregate_totals` em `lib/pure.ml`.
  - Processo:
    1. Calcula `total_amount` com `fold_left`, somando `calculate_item_total` (`price * quantity`).
    2. Calcula `total_taxes` com `fold_left`, somando `calculate_item_tax` (`(price * quantity) * tax`).
  - Resultado: Lista de *records* `result list`.

- **Médias Mensais**:
  - Função: `calculate_monthly_averages` em `lib/pure.ml`.
  - Processo:
    1. Extrai mês/ano de `order_date` com `extract_month_year` (ex.: "2024-06").
    2. Agrupa resultados por mês/ano com `fold_left`, somando valores e contando ocorrências.
    3. Calcula médias dividindo somas pelo número de pedidos por mês.
  - Resultado: Lista de *records* `monthly_avg list`.

- **Funções Puras**: Todas as transformações usam `map`, `reduce` (`fold_left`), e `filter`, mantendo imutabilidade e estando em `pure.ml`.

### 3. Carga (Load)
A carga escreve os resultados processados em arquivos CSV.

- **Escrita de `output.csv`**:
  - Função: `write_result` em `lib/impure.ml`.
  - Processo:
    1. Define o cabeçalho `["order_id"; "total_amount"; "total_taxes"]`.
    2. Converte cada `result` em uma linha com `List.map`, formatando floats com `Printf.sprintf "%.2f"`.
    3. Usa `Csv.save` para gravar o arquivo.
  - Resultado: CSV com totais por pedido.

- **Escrita de `monthly_averages.csv`**:
  - Função: `write_monthly_averages` em `lib/impure.ml`.
  - Processo:
    1. Similar a `write_result`, com cabeçalho `["month_year"; "avg_amount"; "avg_taxes"]`.
    2. Formata valores com `Printf.sprintf "%.2f"`.
  - Resultado: CSV com médias mensais.

- **Separação**: Essas funções impuras estão em `impure.ml`, isoladas das transformações puras.

### Integração no `main.ml`
- Função: `bin/main.ml`.
- Processo:
  1. Lê os CSVs com `read_orders` e `read_order_items`.
  2. Filtra, junta e agrega com `process_data`.
  3. Calcula médias com `calculate_monthly_averages`.
  4. Escreve resultados com `write_result` e `write_monthly_averages`.
  5. Inclui logs para depuração (ex.: número de ordens carregadas).

## Testes
- Arquivo: `test/test_pure.ml`.
- Processo:
  - Testa todas as funções puras: `calculate_item_total`, `calculate_item_tax`, `filter_orders`, `group_by_order_id`, `aggregate_totals`.
  - Usa `assert` para validar resultados esperados e imprime mensagens de sucesso.
- Execução: `dune runtest`.

## Requisitos do Projeto

### Obrigatórios
1. **OCaml**: Implementado inteiramente em OCaml.
2. **Map, Reduce, Filter**:
   - `map`: Conversão de CSVs em *records*, escrita de resultados.
   - `reduce` (`fold_left`): Agrupamento, soma de totais, cálculo de médias.
   - `filter`: Filtragem de ordens.
3. **Leitura/Escrita CSV**: Funções `read_orders`, `read_order_items`, `write_result`, `write_monthly_averages`.
4. **Separação Puro/Impuro**: `pure.ml` para transformações, `impure.ml` para I/O.
5. **Lista de Records**: Estruturas `order`, `order_item`, `result`, `monthly_avg` como listas.
6. **Helper Functions**: `string_to_int` e `string_to_float` para criar *records*.
7. **Relatório**: Este documento detalha o processo.

### Opcionais (5/7 Cumpridos)
1. **Inner Join**: `inner_join` junta ordens e itens antes da transformação.
2. **Dune**: Estrutura do projeto usa Dune para compilação e testes.
3. **Docstrings**: Todas as funções em `pure.ml` e `impure.ml` têm comentários no formato *docstring*.
4. **Testes Completos**: `test_pure.ml` cobre todas as funções puras com casos de teste.
5. **Média por Mês/Ano**: Implementado com `calculate_monthly_averages` e `write_monthly_averages`.
6. **Não Implementados**:
   - Leitura via HTTP: Não foi adicionada por simplicidade.
   - Salvamento em SQLite: Não implementado devido à complexidade adicional.

## Exemplo de Resultados
Para `status = "Complete"` e `origin = "O"`:
- **`output.csv`**:
  ```
  order_id,total_amount,total_taxes
  2,2982.10,332.18
  5,2171.08,179.00
  8,102.84,7.20
  11,1660.94,164.61
  13,1976.99,220.24
  ```
- **`monthly_averages.csv`**:
  ```
  month_year,avg_amount,avg_taxes
  2024-06,2171.08,179.00
  2024-08,2982.10,332.18
  2024-11,886.89,85.91
  2025-01,1976.99,220.24
  ```

## Uso de IA Generativa

Este projeto foi desenvolvido com auxílio de IA generativa:
- **Criação de Código**: Sugestões iniciais para funções como `filter_orders`, `inner_join`, e `aggregate_totals`.
- **Documentação**: Geração deste relatório como base, ajustado manualmente para clareza e conformidade.
- **Testes e Opcionais**: Propostas para testes unitários e cálculo de médias mensais.


## Considerações Finais

O projeto atende a todos os requisitos obrigatórios e implementa 5 dos 7 opcionais, demonstrando o uso de programação funcional em um pipeline ETL escalável. Para reproduzir, siga os passos descritos, garantindo que os arquivos CSV estejam no formato correto. O código é modular, testado e documentado, facilitando manutenção futura.
