# 📊 analise-vendas-game-nft — Análise de Mercado Secundário do Upland

Ferramenta desenvolvida em Python para consulta, processamento e análise das últimas vendas realizadas no mercado secundário do [Upland](https://upland.me), plataforma de metaverso baseada em blockchain.

---

## 📌 Sobre o Projeto

A ferramenta busca dados de vendas recentes diretamente das APIs do jogo e da blockchain, processa as informações e gera uma planilha Excel estruturada com estatísticas e rankings de mercado — fornecendo inteligência para tomada de decisão de compra e venda de propriedades.

---

## ⚙️ Funcionalidades

- **Consulta simultânea** às APIs do jogo e da blockchain para obter dados completos de cada transação
- **Número de vendas configurável** pelo usuário — defina quantas transações deseja analisar
- **Separação por tipo de moeda** — vendas realizadas em UPX (token nativo) e USD tratadas de forma independente
- **Geração de planilha Excel** com as seguintes informações por venda:
  - Endereço da propriedade comercializada
  - Bairro e cidade
  - Preço de venda
  - Markup em relação ao valor de mint
  - Nome do comprador
  - Indicador de propriedade de coleção
  - Indicador de existência de construção no terreno
- **Aba de estatísticas** com tabelas separadas para vendas em UPX e USD
- **Ranking por bairro** — identifica os bairros com maior volume de vendas para cada tipo de moeda

---

## 🛠️ Tecnologias Utilizadas

| Tecnologia | Uso |
|---|---|
| Python | Linguagem principal |
| Requests / aiohttp | Consulta às APIs do jogo e da blockchain |
| openpyxl / pandas | Geração e formatação da planilha Excel |
| web3 | Integração com a blockchain |
| python-dotenv | Gerenciamento seguro de credenciais |

---

## 📁 Estrutura do Projeto

```
analise-vendas-game-nft/
├── consultar_vendas.py       # Script principal
├── neighborhoods_cache.json  # Cache de bairros obtidos via boundaries geográficos
└── requirements.txt          # Dependências do projeto
```

---

## 🚀 Como Executar

**Pré-requisitos:**
- Python 3.x
- Credenciais de autenticação do Upland configuradas em `.env`
- Compatível com Linux e Windows

**Instalação:**

```bash
pip install -r requirements.txt
```

**Execução:**

```bash
python consultar_vendas.py
```

Ao executar, informe o número de vendas que deseja consultar e o tipo de moeda (UPX, USD ou ambos). A planilha será gerada automaticamente na pasta do projeto.

---

## 📈 Exemplo de Saída

A planilha gerada contém:

- **Aba UPX** — listagem detalhada das vendas em UPX com ranking de bairros
- **Aba USD** — listagem detalhada das vendas em USD com ranking de bairros
- **Aba Estatísticas** — tabelas comparativas de volume por bairro e cidade

---

## 🤝 Desenvolvimento

Projeto desenvolvido de forma autônoma, com auxílio de ferramentas de Inteligência Artificial (Claude — Anthropic) como suporte ao desenvolvimento. Todas as decisões de arquitetura, configuração, testes e manutenção foram conduzidas pelo autor.

A identificação de bairros foi implementada via boundaries geográficos em JSON — uma solução desenvolvida antes da descoberta de uma API com autenticação que fornece essa informação diretamente. A abordagem original foi mantida por estar funcional e estável, ilustrando uma decisão consciente de não alterar o que funciona.

---

*Autor: Alexandre Toro Batista — São Paulo, SP*
*Iniciado em 2024*
