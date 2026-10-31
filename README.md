# Fluxo Essencial

Reescrita independente do aplicativo anterior. Uma tela mensal com entradas manuais,
contas, saldo disponível informado e casamento por data. Sem agenda de plantões,
envelopes, gráficos, categorias ou motores antigos de recorrência.

## Regras

- O mês é definido pelo vencimento previsto, inclusive para registros confirmados.
- Previsto e realizado são guardados separadamente. Receber não aumenta o disponível.
- O disponível é um retrato manual atual, compartilhado entre os meses. Cada mês é
  uma simulação independente; as projeções de meses diferentes não podem ser somadas.
- Ao confirmar pagamento, o formulário pede o disponível restante. Confira o valor:
  a sugestão considera o valor previsto. Isso permite registrar também pagamentos
  já feitos sem descontá-los novamente. Pagamento e saldo são salvos na mesma transação.
- Desfazer uma confirmação não altera o saldo manual. A interface pede sua conferência.
- Casamento: contas pendentes por vencimento; disponível hoje, depois entradas por
  data. Renda recebida não é usada outra vez. Renda atrasada aparece como dependência
  incerta; dinheiro posterior ao vencimento gera aviso. Nenhum centavo é alocado duas vezes.
- Não há transferência automática de pendências ou sobras entre meses. Antes de informar
  o disponível, reserve valores para compromissos fora do mês selecionado.
- Repetições têm duração explícita e geram previsões independentes. A edição permite escolher somente a ocorrência ou ela e as próximas pendentes da série. Pagos e recebidos são preservados. Parcelamento divide o valor total com precisão de centavos.
- Edição/exclusão só de pendentes; confirmações podem ser desfeitas. Concorrência usa
  versões para recusar alterações sobre dados desatualizados.

## Base anterior

`python migrate.py` cria **somente** `simple_entries` e `simple_balance`, sem ler,
apagar ou modificar as tabelas anteriores. A nova tela começa vazia. Não há importação
implícita de plantões, rendas ou dívidas antigas, para não carregar duplicidades ou
regras abandonadas. A versão anterior permanece no histórico Git e seus dados no banco.
A transferência de dados deve ser revisada separadamente antes de trocar a produção.

## Railway

Dependências: `pip install -r requirements.txt`.
Pre-Deploy Command: `python migrate.py`.
Start Command: `streamlit run app.py --server.port $PORT --server.address 0.0.0.0`.

Variáveis existentes continuam aceitas: `DATABASE_URL`, `APP_PASSWORD`, ou
`APP_USERS_JSON` com usuários, `password_hash` PBKDF2 e schemas `tenant_...` já criados.
TLS é obrigatório por padrão. `DB_SSLMODE=disable` é destinado somente a banco local de testes.
Nenhum dado financeiro ou credencial deve ser versionado.

## Verificação

`pip install -r requirements-dev.txt` e `python -m pytest -q`.
Para testes reais de PostgreSQL e interface defina `TEST_DATABASE_URL` apontando para
um banco descartável. Os testes criam e apagam schemas próprios nesse banco.
