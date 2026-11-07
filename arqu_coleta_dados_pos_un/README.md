### 1. Bronze — ingestão dos dados

Os arquivos são recebidos no bucket `landing` do MinIO. Depois, o Spark lê esses arquivos e grava as tabelas Bronze no formato Iceberg.

Nessa etapa, os dados são mantidos como chegaram. São adicionadas as colunas `_ingest_ts`, `_source_file` e `_batch_id`, que permitem identificar quando, de onde e em qual lote os dados foram recebidos.

Também foi testada a reexecução da ingestão. As linhas não são duplicadas, pois o lote é apagado e gravado novamente. Porém, a quantidade de objetos no storage pode aumentar porque o Iceberg mantém o histórico de versões.

### 2. Silver — tratamento e qualidade

Na Silver, cada tabela recebe regras específicas:

- **Clientes:** conversão do ID, padronização do estado e do e-mail, validação de datas e manutenção do registro mais recente por cliente.
- **Produtos:** preço e custo precisam ser maiores que zero.
- **Pedidos:** conversão de tipos, remoção de duplicidades e validação da existência do cliente. Os itens de cada pedido são separados em linhas na tabela `itens_pedido`.
- **Avaliações:** a nota precisa estar entre 1 e 5, além da remoção de avaliações duplicadas.

Os registros que não passam pelas validações não são simplesmente descartados: eles são enviados para a tabela de quarentena, junto com a identificação da origem.

Também foi feita uma reconciliação para verificar se os pedidos recebidos estão contabilizados entre os pedidos válidos e os enviados para a quarentena. Os resultados das verificações de qualidade ficam registrados em `dq_resultados`.

### 3. Gold — dados prontos para consumo

São publicadas no PostgreSQL, no schema `gold`, para serem utilizadas no Metabase. A proposta é montar análises de vendas, segmentos de clientes e avaliações.

**Para busca vetorial**, foi criada a tabela `avaliacao_chunks`, com trechos de avaliações preparados para uma futura busca por significado. Antes de gerar os vetores, o processo limpa os textos, descarta conteúdos muito curtos, mascara possíveis dados pessoais, cria um identificador estável (`chunk_id`) e calcula um hash (`content_hash`).

### 1. Qual regra da Silver mandou mais registros para a quarentena e por quê?

A regra que mais enviou registros foi `pedidos_cliente_orfao`, com **40 registros**. Isso acontece quando um pedido está relacionado a um `cliente_id` que não existe na tabela de clientes. Também foram para a quarentena 25 avaliações com nota inválida e 3 produtos com preço ou custo inválido.

### 2. Se a reconciliação não fechasse, o que você investigaria primeiro?

Eu começaria conferindo as regras da Silver e o trecho do código responsável por separar os registros válidos dos que vão para a quarentena. Também verificaria a deduplicação dos pedidos e se todos os registros rejeitados estão sendo gravados corretamente. A conta esperada é: pedidos que entraram = pedidos válidos na Silver + pedidos na quarentena.

### 3. Que dado pessoal existe na sua arquitetura e como ele foi tratado?

Na parte de clientes, existe o campo de e-mail. Na Silver, ele é padronizado para letras minúsculas e, quando é inválido, recebe `NULL`. Já na preparação dos textos para busca vetorial, existe uma função para mascarar possíveis dados pessoais, como CPF, telefone e e-mail, antes de enviar o conteúdo para o índice vetorial. O notebook demonstra essa máscara com textos de exemplo e observa que os dados de avaliação usados na aula não possuem esses dados pessoais. Não identifiquei nos notebooks uma regra explícita de remoção ou restrição de acesso para todos os campos pessoais das tabelas de BI.

### 4. O que você mudaria nesta arquitetura se o volume de dados fosse 1.000 vezes maior?

A primeira mudança seria evitar que a Silver precisasse ler novamente toda a Bronze a cada execução. Eu implementaria um processamento incremental, controlando os lotes já processados para trabalhar somente com dados novos ou alterados. Também aumentaria os recursos do Spark e adicionaria um orquestrador para controlar as etapas, falhas e logs. Para as consultas da Gold em grande volume, avaliaria um banco mais adequado para análise (OLAP), em vez de depender apenas do PostgreSQL.

