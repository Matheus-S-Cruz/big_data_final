# Relatório — datalake de tracking da DadosPro

Este relatório responde às quatro perguntas de negócio do enunciado a partir das
tabelas Gold geradas pelo pipeline (`pipeline/gold_*.py`). Cada resposta cita a
tabela e os números que a sustentam e termina com uma recomendação. A seção final
documenta a qualidade do dado.

Os números abaixo referem-se à execução do pipeline sobre os 14 dias de eventos
gerados (`gerador/gerar_eventos.py`), com **61.813 eventos aprovados** na Silver.

---

## 1. Funil — onde a jornada de compra perde mais gente?

**Tabela:** `gold/funil_compra` (linha `(total)`) e `gold/funil_lead`.

Funil de compra, por sessão (uma sessão conta numa etapa se teve o evento em
qualquer momento — definição 5.4.4):

| Etapa | Sessões | Conversão sobre o topo | Conversão sobre a etapa anterior |
| --- | --- | --- | --- |
| view_item_list | 5.258 | 100,0% | — |
| select_item | 1.719 | 32,7% | **32,7%** |
| add_to_cart | 1.291 | 24,6% | 75,1% |
| view_cart | 1.255 | 23,9% | 97,2% |
| begin_checkout | 912 | 17,3% | 72,7% |
| add_payment_info | 684 | 13,0% | 75,0% |
| purchase | 573 | 10,9% | 83,8% |

A taxa de `view_item` (abrir o detalhe do plano, fora da sequência) é de **57,3%**
sobre quem fez `select_item`.

**Leitura:** a maior perda do funil, de longe, está no primeiro passo —
**`view_item_list → select_item`, com apenas 32,7% de conversão**. De cada 100
sessões que veem a tabela de planos, só ~33 chegam a clicar num plano; as outras
~67 abandonam sem escolher nada. As etapas seguintes convertem bem (72% a 97%):
quem escolhe um plano tende a seguir até o fim (o `view_cart → begin_checkout` cai
para 72,7%, a segunda maior perda, já dentro do checkout).

No funil de lead (`funil_lead`), a conversão de `scroll ≥ 50% → generate_lead` é
baixa em todos os canais (entre 3,8% e 9,8%), sendo pior no `newsletter / email`
(3,77%).

**Recomendação:** concentrar esforço na transição **planos → seleção**. A página
mostra os planos (5.258 sessões), mas a decisão trava ali. Vale testar uma tabela
de planos mais clara, destaque de um plano recomendado, prova social junto aos
cards e um CTA mais direto em cada plano. Ganhos nesse ponto se propagam por todo
o resto do funil, que já converte bem a partir da seleção.

---

## 2. Engajamento — o vídeo é visto até o fim? O que falta no FAQ?

**Tabelas:** `gold/retencao_video`, `gold/buscas_faq`, `gold/saidas_downloads`.

Retenção do vídeo de apresentação (VSL), entre as **2.890 sessões** que deram play:

| Marco | Sessões que atingiram | % retenção |
| --- | --- | --- |
| 10% | 2.626 | 90,9% |
| 25% | 2.138 | 74,0% |
| 50% | 1.474 | 51,0% |
| 75% | 1.105 | 38,2% |
| 100% | 877 | **30,4%** |

Buscas no FAQ sem nenhum resultado (`buscas_faq`, `buscas_sem_resultado`):

| Termo | Buscas | Sem resultado |
| --- | --- | --- |
| estagio | 44 | 44 (100%) |
| presencial | 33 | 33 (100%) |
| bolsa | 32 | 32 (100%) |
| power bi | 28 | 28 (100%) |
| linux | 24 | 24 (100%) |

Termos como `parcelar`, `acesso`, `certificado`, `reembolso` e `apostila`
retornam resultado (0 buscas sem resultado).

**Leitura:** o vídeo **perde metade da audiência até a metade** (51% em 50%) e só
**30,4% assistem até o fim**. A maior queda acontece logo no início (de 10% para
25%, caem ~490 sessões). No FAQ, há uma demanda recorrente **sem resposta**:
estágio, curso presencial, bolsa, Power BI e Linux — temas que o visitante
procura e não encontra.

**Recomendação:** encurtar/reorganizar o começo do vídeo para segurar a audiência
nos primeiros 25%, onde está a maior evasão. No FAQ, criar entradas para os termos
mais buscados sem resultado (estágio, presencial, bolsa, Power BI, Linux): são
perguntas de intenção clara que hoje geram frustração e possível abandono.

---

## 3. Receita e aquisição — quanto entrou, e qual canal traz cliente que fica?

**Tabelas:** `gold/receita_diaria`, `gold/taxa_reembolso`, `gold/adesao_order_bump`,
`gold/lead_para_compra`.

Pedidos e reembolso por plano/canal (`taxa_reembolso`), consolidado por plano:

| Plano | Pedidos | Taxa de reembolso (faixa por canal) |
| --- | --- | --- |
| PLANO-BASICO | 193 | 0% (direct, newsletter) a 19,2% (instagram) |
| PLANO-PRO | 260 | 2,6% (newsletter) a 16,7% (instagram) |
| PLANO-PREMIUM | 120 | 0% (direct) a **40,0% (instagram)** |

Order bump (`adesao_order_bump`): **90 de 573 pedidos** levaram a Mentoria em grupo
→ **adesão de 15,7%**.

Lead → compra (`lead_para_compra`): de **522 leads**, **40 compraram depois**
(7,7%), com mediana de **4,64 dias** entre o lead e a compra. Por canal do lead:

| Canal do lead | Leads | Compraram | Mediana (dias) |
| --- | --- | --- | --- |
| google / cpc | 165 | 16 (9,7%) | 3,54 |
| google / organic | 118 | 8 (6,8%) | 1,89 |
| direct / (none) | 94 | 8 (8,5%) | 7,48 |
| instagram / paid_social | 131 | 7 (5,3%) | 6,05 |
| newsletter / email | 14 | 1 (7,1%) | 10,09 |

**Leitura:** o canal **`instagram / paid_social` tem a pior qualidade de cliente**
— a maior taxa de reembolso em todos os planos (chegando a **40% no Premium**) e a
menor conversão de lead em compra (5,3%). Já o **`google / cpc`** combina o maior
volume de leads (165) com a melhor conversão lead→compra (9,7%), e o
**`google / organic`** converte rápido (mediana de 1,89 dia). O order bump tem
adesão saudável (15,7%).

**Recomendação:** rever a segmentação e a promessa das campanhas de Instagram —
elas atraem cliente que compra e devolve, corroendo a receita líquida (há casos de
receita líquida zerada no Premium via Instagram). Reinvestir no `google / cpc`, que
traz cliente que fica, e explorar o `google / organic`, que decide depressa.

---

## 4. Previsão — quais sessões estão perto de comprar, e que perfis existem?

**Tabelas:** `gold/propensao_sessao`, `gold/metricas_modelo`, `gold/segmentos_visitantes`.

### Propensão de compra

Modelo de classificação (regressão logística) que estima a probabilidade de a
sessão terminar em compra. Para evitar **vazamento de informação**, as features só
usam o comportamento na landing **antes do primeiro `add_to_cart`** da sessão
(rolagem, vídeo, planos vistos, plano selecionado, detalhe aberto, buscas,
downloads, cliques, canal e dispositivo). Nenhum evento do checkout entra.

Métricas no conjunto de teste (`metricas_modelo`), referentes à classe positiva
(sessão com compra):

| Métrica | Valor |
| --- | --- |
| AUC | 0,934 |
| F1 (classe positiva) | 0,494 |
| Precisão (classe positiva) | 0,329 |
| Recall (classe positiva) | 0,990 |

> **Nota sobre o balanceamento (ver seção de qualidade):** compradores são raros
> (~5% das sessões). Sem balanceamento, precisão e recall da classe positiva
> saíam **0,0** mesmo com AUC alto — o modelo ordenava bem, mas nunca cruzava o
> corte de 0,5 e previa sempre "não comprou". Com **peso de classe**
> (inversamente proporcional à frequência), o modelo passou a apontar
> compradores, e as métricas acima são da versão balanceada.

O AUC de **0,934** indica que o modelo **ranqueia muito bem**: dado um par
(comprador, não-comprador), ele acerta a ordem em ~93% dos casos. Depois do
balanceamento, o **recall é de 0,990** — o modelo praticamente não deixa escapar
um comprador — ao custo de uma **precisão de 0,329**: entre as sessões que ele
aponta como prováveis compras, cerca de 1 em 3 compra de fato. Esse é o
compromisso esperado num problema desbalanceado e é o perfil certo para
*remarketing*: prefere-se abordar alguns não-compradores a mais (precisão menor) a
perder compradores reais (recall alto). Na prática, priorize as sessões com maior
`probabilidade_compra` para ação comercial.

### Segmentos de visitantes

Clusterização por K-Means (k=4) sobre o comportamento pré-carrinho, silhueta de
**0,40**. Perfil médio de cada grupo (`segmentos_visitantes` / `_perfil`):

| Grupo | Visitantes | Scroll méd. | Vídeo méd. | Viu planos | Selecionou | Downloads | Perfil |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 2.057 | 23,1 | 10,6 | 0,11 | 0,04 | 0,00 | **Passante** — entra, rola pouco e sai |
| 1 | 1.050 | 66,6 | 18,4 | 0,92 | **0,86** | 0,00 | **Comprador em potencial** — vê e seleciona plano |
| 2 | 2.615 | 73,0 | 21,4 | 0,85 | 0,04 | 0,00 | **Explorador** — consome conteúdo, não seleciona |
| 3 | 272 | 71,0 | 18,3 | 0,83 | 0,23 | **0,81** | **Pesquisador** — baixa a ementa, compara |

**Leitura:** o maior grupo é o de **passantes** (grupo 0, ~2.000 visitantes) com
engajamento mínimo. O grupo mais valioso é o **1** (~1.050): rola bastante, vê os
planos e, sobretudo, **seleciona um plano em 86% dos casos** — é o público mais
próximo da compra. O grupo **2** engaja muito (maior scroll e vídeo) mas quase não
seleciona plano: interesse alto, intenção de compra baixa. O grupo **3** é pequeno
mas distinto: quase todos baixam a ementa (0,81), um comportamento de quem compara
antes de decidir.

**Recomendação:** usar o score de `propensao_sessao` e os segmentos juntos. Para o
grupo 1 (alta seleção), remover atritos do checkout e oferecer o order bump. Para o
grupo 2 (muito engajamento, pouca seleção), testar CTAs mais diretos para a tabela
de planos. Para o grupo 3 (pesquisadores), um e-mail de follow-up pós-download da
ementa, já que decidem com mais informação.

---

## Qualidade do dado

### Deduplicação (R5)

O coletor reenvia lotes, então o mesmo `event_id` chega mais de uma vez. Na Silver:

- Eventos na Bronze (append de execuções): **193.116**
- Após deduplicação por `event_id`: **63.716**
- **Duplicatas descartadas: 129.400**

> O número da Bronze acumula execuções anteriores (a Bronze é append-only, R4);
> a deduplicação por `event_id` garante uma linha por evento independente disso.

### Rejeições com motivo (R7)

Eventos recusados, gravados em `silver/eventos_rejeitados` com um motivo por linha
(**1.903 registros de rejeição**):

| Motivo | Registros |
| --- | --- |
| timestamp ausente ou futuro | 948 |
| valor monetário inválido | 349 |
| marco fora do padrão | 272 |
| evento fora do catálogo | 271 |
| reembolso sem compra | 32 |
| transação sem id | 31 |

### Alguma regra rejeitou dado legítimo?

Não de forma relevante. As regras seguem o contrato do evento (seção 5.2 do
enunciado): marcos de scroll só em {25, 50, 75, 90}, de vídeo só em
{0, 10, 25, 50, 75, 100}, `currency` obrigatório `BRL` quando há `value`, etc. Um
ponto de atenção é "reembolso sem compra" (32 casos): são reembolsos cujo
`transaction_id` não casa com nenhuma compra **válida**. Se a compra
correspondente tiver sido rejeitada por outro motivo, o reembolso legítimo também
cai — é um efeito em cascata esperado, e o volume é pequeno.

### Armadilhas encontradas e como foram tratadas

1. **Duplicatas de lote (reenvio do coletor).** O mesmo `event_id` aparece várias
   vezes. Tratada com `row_number()` por `event_id` ordenado por instante de
   ingestão, mantendo uma cópia (R5). Impacto grande: ~129 mil linhas removidas.

2. **`session_hint` não confiável.** O campo muda a cada recarga de página e não
   pode ser usado (R8). As sessões foram calculadas do zero com window functions,
   cortando quando o mesmo `user_pseudo_id` fica > 30 min sem evento.

3. **Identidade partida entre anônimo e logado.** Eventos anteriores ao login não
   têm `user_id`. O identity stitching (R9) propaga o `user_id` resolvido para
   todos os eventos do mesmo `user_pseudo_id`, inclusive os anteriores ao login.

4. **Desbalanceamento de classe no modelo de propensão.** Compradores são ~5% das
   sessões. Sem tratamento, o modelo prevê sempre "não comprou" (precisão e recall
   da classe positiva = 0, apesar de AUC alto). Tratada com peso de classe no
   treino, para o modelo efetivamente apontar compradores. Documentado na pergunta 4.

5. **Vazamento de informação no modelo.** Eventos do checkout (`view_cart` em
   diante) praticamente revelam a compra. As features foram restritas ao
   comportamento **antes do primeiro `add_to_cart`** da sessão, como exige a
   seção 5.3.

6. **Modo ANSI do Spark 4 (erro em vez de null).** Conversões de texto para
   timestamp/número e divisões por zero param o job por padrão. Usei `try_to_timestamp`
   para rejeitar timestamps mal formados (em vez de quebrar) e `try_divide` em todas
   as taxas da Gold cujo denominador pode ser zero.
