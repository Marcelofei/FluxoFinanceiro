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

Em **Fluxo → Reorganizar contas pendentes**, um clique desconsidera automaticamente as entradas do mês selecionado já marcadas como recebidas. O casamento passa a usar apenas contas e rendas desse mês, definido pela data de vencimento. Outros meses não participam nem têm suas preferências alteradas. O histórico dos lançamentos é preservado. A ação pode ser desfeita no mesmo mês. Novos recebimentos são desconsiderados ao clicar novamente. Preferências da antiga seleção global não são aplicadas ao novo modo mensal. Não é necessário selecionar recebimentos nem informar saldo bancário.

## Editar ou excluir fontes de renda

Em Rendas, Excluir fonte remove o cadastro e os plantões com data de trabalho a partir do dia da exclusão, após confirmação. O corte usa a data do plantão, não a data de recebimento. Plantões anteriores e lançamentos que não são plantões são preservados. Os registros removidos ficam documentados em auditoria. A fonte excluída não reaparece como importada por causa dos lançamentos antigos.

Alterar Dia de recebimento ou Meses até receber atualiza, na mesma transação, os lançamentos não recebidos dessa fonte cujo vencimento esteja no mês atual ou posterior. O mês atual é o mês de hoje, mesmo se outro mês estiver selecionado. A nova data usa a competência original acrescida do prazo; dias 29–31 são limitados ao último dia do mês. Baixas confirmadas, valores personalizados e pendências anteriores ao mês atual são preservados. Previsões que ainda usam o padrão antigo acompanham o novo valor. A cobertura é recalculada com as novas datas. Novas recorrências também respeitam o prazo configurado. Alterações ficam registradas em auditoria.

## Agenda de plantões e fontes de renda

O total da fonte vem dos plantões com recebimento previsto no mês selecionado; o mês do trabalho continua visível na agenda. O valor por plantão não substitui o total mensal quando a agenda está vazia. Plantões antigos com outra categoria são associados visualmente à fonte pelo hospital quando existe uma única correspondência, sem reescrever o histórico. Hospitais diferentes permanecem separados na cobertura.

Ver plantões abre a agenda da fonte. Salvar valores dos plantões atualiza os registros pendentes e, automaticamente, a previsão de renda; valores já recebidos são preservados. O CSV respeita também prazo de zero meses. Alterar a data da fonte alcança plantões antigos do mesmo hospital quando a identificação é inequívoca.

Para fontes marcadas como Plantões, o campo de valor em Rendas é somente leitura. O total vem da agenda. Adicionar, alterar ou excluir um plantão atualiza a previsão automaticamente, no mês previsto de recebimento.


## Restauração do app completo

Esta atualização retoma o código completo do commit
`7b7018c7934334e7fc12bc411d1d81b36bcd0ca8`, imediatamente anterior ao PR #8
que introduziu o app simplificado. A lógica funcional é a mesma dessa versão;
correções seguintes serão feitas separadamente, uma por vez.

O código completo usa as tabelas antigas (`lancamentos`, categorias, pagamentos,
faturas etc.). As tabelas `simple_entries`, `simple_balance` e `simple_reminders`
permanecem no banco, mas não são exibidas nem migradas pelo aplicativo completo.
Voltar o código não transfere dados automaticamente entre os dois modelos.
Se a DATABASE_URL e o schema continuarem os mesmos e as tabelas antigas ainda
existirem, o app completo volta a consultá-las. Sem acesso ao banco em produção,
não é possível garantir quais registros existem atualmente.

Mantenha `python migrate.py` no Pre-Deploy e o comando de inicialização Streamlit
existente. Antes da troca, guarde uma cópia do banco atual pelo provedor. Não
restaure um ZIP antigo sobre o banco atual para apenas trocar a interface: o
restaurador de backups do app completo substitui os dados das tabelas que gerencia.
Os testes de compatibilidade verificam a preservação das tabelas simplificadas
em uma base já migrada e em uma base que recebe as migrações antigas pela primeira vez.


## Correções de integridade v26

Esta versão exige `python migrate.py` antes de iniciar o app (schema 4). No Railway, configure esse comando como **Pre-deploy Command**; mantenha o comando de início existente. A migração cria metadados de versão, vínculo de fonte, exceções mensais e regras das séries, preservando os valores e as baixas existentes. Backups novos usam formato 5 e incluem essas regras; formatos anteriores continuam aceitos.

- A lista operacional fica restrita ao mês selecionado, independentemente do botão Reorganizar.
- O gerador materializa pelo mês de recebimento consultado, inclusive fontes com atraso. Datas legadas inferidas deixam de aplicar o atraso duas vezes.
- Alterar o padrão da fonte atualiza os pendentes que ainda usam o padrão anterior; recebidos e exceções individuais são preservados.
- Editor rápido verifica a versão atual e usa o mesmo serviço de baixa/estorno da linha do tempo. Campo real vazio usa o planejado; zero explícito continua válido. Número inválido e baixa futura são rejeitados.
- “Este e futuros” atualiza a regra e os pendentes; excluir futuros encerra a regra. Séries criadas em Novo lançamento não têm mais horizonte fixo de 60 meses e aparecem em Recorrências.
- Dívidas mostram a soma das obrigações abertas, sem saldo artificial por desconto de parcela integralmente liquidada.
- Ajustes de fatura não podem ser baixados, estornados ou excluídos separadamente da baixa original.

Validação: suíte de regressão, testes de navegação, concorrência entre baixas e 1.500 cenários determinísticos de rateio/parcelamento. A suíte completa usa a instância PostgreSQL 16 descartável do GitHub Actions. Não use TEST_DATABASE_URL apontando para produção.
