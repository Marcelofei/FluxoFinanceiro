# FluxoFinanceiro

Agenda financeira para relacionar rendas e contas cadastradas, com pouco trabalho de manutenção. A cobertura é uma projeção baseada nesses registros, não um saldo bancário; não exige conciliação ou cadastro de cada pequeno gasto.

## Executar

Python 3.12 e PostgreSQL. Instale `requirements.txt`, configure `DATABASE_URL` e a autenticação, execute `python migrate.py` e então `streamlit run app.py`. A migração deve rodar como etapa de release antes da aplicação. Faça backup antes da atualização e valide a migração em uma cópia. A aplicação bloqueia acesso com migrações pendentes.

O modo privado aceita `APP_PASSWORD`. Para contas separadas, configure `APP_USERS_JSON` conforme `security.py`: cada conta tem `password_hash` e um `schema` próprio. Gere hashes com `new_password_hash`; execute migrações para todas as contas antes de abrir o app. O banco continua usando uma credencial de serviço compartilhada; o isolamento por schema depende da aplicação.

## Uso e integridade

- **Desfazer pagamento/recebimento** reabre a obrigação e preserva o valor planejado. O histórico em Fluxo mostra as últimas 100 alterações, com data, ator e valores. Eventos completos integram o backup.
- **Repetição de clique:** cada rascunho possui uma chave persistida no banco por parcela. Uma nova operação intencional com os mesmos dados usa o botão **Novo lançamento**. Operações diferentes continuam livres.
- **Importação e escala de plantões:** local e data identificam o plantão. Reimportações e gerações simultâneas preservam registros existentes, inclusive valores pagos. Este fluxo considera um plantão por local/dia; dois turnos no mesmo local/dia precisam ser diferenciados antes de ampliar essa regra.
- **Recorrências:** a reserva de categoria/competência e o lançamento ocorrem na mesma transação, protegendo contra duas sessões simultâneas.
- **Situação em DD/MM/AAAA:** usa a data atual em São Paulo, mesmo ao consultar outro mês. Distingue baixas confirmadas de previsões futuras; não simula o estado histórico de um dia passado.
- Cartões/faturas possuem identidade própria. Encargos e descontos não alteram as categorias das compras; ao desfazer, o ajuste vinculado à baixa é removido com registro em auditoria.
- Backups ZIP são restaurações integrais, não importações incrementais; a restauração é transacional. CSV legado não contém todos os vínculos e configurações novos.

## Testes

`pip install -r requirements-dev.txt` e `python -m pytest -q`.

Defina `TEST_DATABASE_URL` para uma base **descartável**: os testes removem e recriam os schemas `tenant_test`/`tenant_other`. Sem essa variável, os testes de integração são pulados. O GitHub Actions usa PostgreSQL 16 descartável e executa a suíte completa.

## Reorganizar o casamento das contas

Em **Fluxo → Reorganizar contas pendentes**, selecione os recebimentos que não estão mais disponíveis (por exemplo, Hospital A) e aplique. O app mantém o histórico e redistribui as pendências por vencimento entre as outras entradas. Valores não cobertos e rendas que chegam depois do vencimento continuam sinalizados. A seleção fica salva por cliente e pode ser desfeita pelo botão **Desfazer reorganização** (última alteração). Novos recebimentos não são automaticamente descartados, mesmo quando pertencem ao mesmo hospital. A ação não paga, cancela ou altera valores dos lançamentos e não exige informar saldo bancário.
