#!/usr/bin/env python3
"""
Gerador de dados sintéticos - caso "MercadoNordeste" (e-commerce regional).

Gera dados SUJOS DE PROPÓSITO para os alunos limparem na camada Prata:
  - e-mails inválidos/nulos, estados com caixa inconsistente, datas em formatos mistos
  - pedidos duplicados (reentrega de arquivo), notas fora do intervalo, preço negativo

Uso:
  python gerar_dados.py --saida ../data            # gera lote1 e lote2
  python gerar_dados.py --saida ../data --seed 7   # outra semente

Lote 1: carga histórica (jan/2025 a jun/2026)
Lote 2: carga incremental (jul/2026) + atualizações de status de pedidos antigos
        (serve para demonstrar MERGE INTO, snapshots e time travel no Iceberg)
"""
import argparse
import csv
import json
import os
import random
from datetime import datetime, timedelta

CIDADES = {
    "PB": ["João Pessoa", "Campina Grande", "Patos", "Cabedelo"],
    "PE": ["Recife", "Olinda", "Caruaru", "Petrolina"],
    "RN": ["Natal", "Mossoró", "Parnamirim"],
    "CE": ["Fortaleza", "Juazeiro do Norte", "Sobral"],
    "BA": ["Salvador", "Feira de Santana", "Vitória da Conquista"],
    "AL": ["Maceió", "Arapiraca"],
    "SE": ["Aracaju", "Lagarto"],
}
CANAIS = ["organico", "google_ads", "instagram", "indicacao", "email"]
CATEGORIAS = {
    "Eletrônicos": ["Fone Bluetooth", "Carregador Turbo", "Smart Speaker", "Power Bank", "Mouse sem fio"],
    "Casa": ["Panela de Pressão", "Jogo de Cama", "Liquidificador", "Cafeteira", "Ventilador"],
    "Moda": ["Camiseta Algodão", "Tênis Casual", "Boné", "Bermuda", "Sandália"],
    "Esporte": ["Garrafa Térmica", "Tapete Yoga", "Halteres 5kg", "Bola de Futebol", "Corda de Pular"],
    "Alimentos": ["Café Torrado 500g", "Castanha de Caju", "Tapioca Pronta", "Cachaça Artesanal", "Doce de Leite"],
}
STATUS = ["entregue", "entregue", "entregue", "entregue", "enviado", "cancelado", "devolvido", "pago"]
PAGTO = ["pix", "pix", "cartao_credito", "cartao_credito", "boleto", "cartao_debito"]
NOMES = ["Ana", "João", "Maria", "Pedro", "Lucas", "Beatriz", "Gabriel", "Larissa", "Rafael", "Camila",
         "Mateus", "Juliana", "Felipe", "Fernanda", "Thiago", "Aline", "Bruno", "Patrícia", "Diego", "Carla"]
SOBRENOMES = ["Silva", "Santos", "Oliveira", "Souza", "Lima", "Pereira", "Costa", "Ribeiro", "Alves", "Barbosa",
              "Cavalcanti", "Medeiros", "Nascimento", "Araújo", "Ferreira", "Gomes"]

COMENT_POS = ["Produto excelente, chegou antes do prazo.", "Muito bom, recomendo para todos.",
              "Qualidade acima do esperado, valeu cada centavo.", "Entrega rápida e produto igual ao anunciado.",
              "Adorei! Já comprei de novo.", "Ótimo custo-benefício, embalagem perfeita."]
COMENT_NEU = ["Produto ok, nada demais.", "Atende ao básico, entrega demorou um pouco.",
              "Esperava um pouco mais pela descrição.", "Razoável pelo preço."]
COMENT_NEG = ["Produto chegou com defeito e o suporte demorou a responder.", "Péssima qualidade, não recomendo.",
              "Atrasou muito a entrega e veio amassado.", "Não funcionou, tive que pedir reembolso.",
              "Cor diferente da foto, fiquei decepcionado."]


def fmt_data_suja(d, rnd):
    r = rnd.random()
    if r < 0.80:
        return d.strftime("%Y-%m-%d")
    if r < 0.92:
        return d.strftime("%d/%m/%Y")
    if r < 0.97:
        return d.strftime("%Y-%m-%dT%H:%M:%S")
    return ""  # nulo


def gerar(saida, seed):
    rnd = random.Random(seed)
    os.makedirs(f"{saida}/lote1", exist_ok=True)
    os.makedirs(f"{saida}/lote2", exist_ok=True)

    # ---------------- produtos (iguais nos dois lotes) -------------------
    produtos = []
    pid = 1
    for cat, nomes in CATEGORIAS.items():
        for n in nomes:
            for var in ("Basic", "Plus", "Pro", "Max"):
                for _ in range(2):
                    preco = round(rnd.uniform(19, 900), 2)
                    produtos.append({"produto_id": pid, "nome": f"{n} {var}", "categoria": cat,
                                     "preco": preco, "custo": round(preco * rnd.uniform(0.45, 0.8), 2)})
                    pid += 1
    # sujeira: 3 preços negativos
    for p in rnd.sample(produtos, 3):
        p["preco"] = -abs(p["preco"])
    prod_ok = {p["produto_id"]: p for p in produtos}

    # ---------------- clientes lote 1 ------------------------------------
    inicio = datetime(2025, 1, 1)
    fim1 = datetime(2026, 6, 30)
    clientes, atividade = [], {}
    for cid in range(1, 5001):
        uf = rnd.choice(list(CIDADES))
        nome = f"{rnd.choice(NOMES)} {rnd.choice(SOBRENOMES)}"
        cad = inicio + timedelta(days=rnd.randint(0, 520))
        email = f"{nome.lower().replace(' ', '.')}{cid}@exemplo.com"
        r = rnd.random()
        if r < 0.03:
            email = email.replace("@", "")   # inválido
        elif r < 0.05:
            email = ""                        # nulo
        uf_sujo = uf if rnd.random() < 0.8 else rnd.choice([uf.lower(), f" {uf} "])
        clientes.append({"cliente_id": cid, "nome": nome, "email": email,
                         "cidade": rnd.choice(CIDADES[uf]), "estado": uf_sujo,
                         "data_cadastro": fmt_data_suja(cad, rnd), "canal_aquisicao": rnd.choice(CANAIS)})
        # propensão a comprar e "data de abandono" (sinal de churn p/ ML)
        freq = rnd.choice([0.2, 0.5, 1, 1, 2, 3])          # pedidos/mês
        abandona = cad + timedelta(days=rnd.randint(30, 700)) if rnd.random() < 0.35 else None
        atividade[cid] = (cad, freq, abandona)

    def gerar_pedidos(ids, ini, fim, prox_pid):
        peds = []
        dias = (fim - ini).days
        n_alvo = int(len(ids) * 6) if dias > 200 else 4000
        tentativas = 0
        while len(peds) < n_alvo and tentativas < n_alvo * 20:
            tentativas += 1
            cid = rnd.choice(ids)
            cad, freq, aband = atividade[cid]
            d = ini + timedelta(days=rnd.randint(0, dias), seconds=rnd.randint(0, 86399))
            if d < cad or (aband and d > aband):
                continue
            if rnd.random() > min(1.0, freq / 3 + 0.15):
                continue
            itens = []
            for _ in range(rnd.choices([1, 2, 3, 4], [55, 28, 12, 5])[0]):
                p = rnd.choice(produtos)
                itens.append({"produto_id": p["produto_id"], "qtd": rnd.randint(1, 3),
                              "preco_unit": abs(p["preco"])})
            peds.append({"pedido_id": prox_pid + len(peds), "cliente_id": cid,
                         "data_pedido": d.strftime("%Y-%m-%dT%H:%M:%S"),
                         "status": rnd.choice(STATUS), "forma_pagamento": rnd.choice(PAGTO),
                         "valor_frete": round(rnd.choice([0, 0, 9.9, 14.9, 24.9]), 2), "itens": itens})
        return peds

    pedidos1 = gerar_pedidos(list(range(1, 5001)), inicio, fim1, 100001)
    # sujeira: duplicatas (reentrega) e cliente órfão
    dups = rnd.sample(pedidos1, 150)
    pedidos1_arq = pedidos1 + [json.loads(json.dumps(d)) for d in dups]
    for p in rnd.sample(pedidos1_arq, 40):
        p["cliente_id"] = 999999   # órfão
    rnd.shuffle(pedidos1_arq)

    def gerar_avaliacoes(peds, prox):
        avs = []
        for p in peds:
            if p["status"] not in ("entregue", "devolvido") or rnd.random() > 0.35:
                continue
            it = rnd.choice(p["itens"])
            nota = rnd.choices([1, 2, 3, 4, 5], [8, 7, 15, 30, 40])[0]
            if p["status"] == "devolvido":
                nota = rnd.choice([1, 2])
            com = rnd.choice(COMENT_POS if nota >= 4 else COMENT_NEU if nota == 3 else COMENT_NEG)
            cat = prod_ok[it["produto_id"]]["categoria"]
            if rnd.random() < 0.5:
                com += f" ({cat})"
            d = datetime.fromisoformat(p["data_pedido"]) + timedelta(days=rnd.randint(3, 20))
            avs.append({"avaliacao_id": prox + len(avs), "pedido_id": p["pedido_id"], "cliente_id": p["cliente_id"],
                        "produto_id": it["produto_id"], "nota": nota, "comentario": com,
                        "data_avaliacao": d.strftime("%Y-%m-%d")})
        for a in rnd.sample(avs, min(25, len(avs))):
            a["nota"] = rnd.choice([0, 6, 10])   # fora do intervalo 1..5
        return avs

    aval1 = gerar_avaliacoes(pedidos1, 1)

    # ---------------- lote 2: incremental + atualizações -----------------
    fim2 = datetime(2026, 7, 31)
    novos = []
    for cid in range(5001, 5601):
        uf = rnd.choice(list(CIDADES))
        nome = f"{rnd.choice(NOMES)} {rnd.choice(SOBRENOMES)}"
        cad = datetime(2026, 7, 1) + timedelta(days=rnd.randint(0, 29))
        novos.append({"cliente_id": cid, "nome": nome, "email": f"{nome.lower().replace(' ', '.')}{cid}@exemplo.com",
                      "cidade": rnd.choice(CIDADES[uf]), "estado": uf,
                      "data_cadastro": cad.strftime("%Y-%m-%d"), "canal_aquisicao": rnd.choice(CANAIS)})
        atividade[cid] = (cad, rnd.choice([1, 2, 3]), None)
    # um cliente antigo muda de cidade (candidato a SCD/UPDATE)
    mudou = dict(clientes[9]); mudou["cidade"] = "Recife"; mudou["estado"] = "PE"
    clientes2 = novos + [mudou]

    pedidos2 = gerar_pedidos(list(range(1, 5601)), datetime(2026, 7, 1), fim2, 100001 + len(pedidos1))
    atualiza = []
    for p in rnd.sample([p for p in pedidos1 if p["status"] in ("pago", "enviado")], 400):
        q = json.loads(json.dumps(p))
        q["status"] = rnd.choice(["entregue", "entregue", "cancelado"])
        atualiza.append(q)
    pedidos2_arq = pedidos2 + atualiza
    aval2 = gerar_avaliacoes(pedidos2, len(aval1) + 1)

    # ---------------- escrita --------------------------------------------
    def w_csv(path, rows):
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader(); w.writerows(rows)

    def w_jsonl(path, rows):
        with open(path, "w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

    w_csv(f"{saida}/lote1/clientes.csv", clientes)
    w_csv(f"{saida}/lote1/produtos.csv", produtos)
    w_jsonl(f"{saida}/lote1/pedidos.jsonl", pedidos1_arq)
    w_csv(f"{saida}/lote1/avaliacoes.csv", aval1)
    w_csv(f"{saida}/lote2/clientes.csv", clientes2)
    w_jsonl(f"{saida}/lote2/pedidos.jsonl", pedidos2_arq)
    w_csv(f"{saida}/lote2/avaliacoes.csv", aval2)

    print(f"lote1: {len(clientes)} clientes, {len(produtos)} produtos, {len(pedidos1_arq)} pedidos (c/ duplicatas), {len(aval1)} avaliações")
    print(f"lote2: {len(clientes2)} clientes, {len(pedidos2_arq)} pedidos (inclui {len(atualiza)} atualizações), {len(aval2)} avaliações")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--saida", default="../data")
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()
    gerar(a.saida, a.seed)
