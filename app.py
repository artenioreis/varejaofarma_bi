import os
import json
import pandas as pd
import plotly
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from flask import Flask, render_template, request, redirect, url_for, flash
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from sqlalchemy import text, create_engine

app = Flask(__name__)
app.config['SECRET_KEY'] = 'varejao-farma-bi-2025-v-final'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///varejaofarma.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
login_manager = LoginManager(app)
login_manager.login_view = 'login'

class DatabaseConfig(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    server = db.Column(db.String(200))
    database = db.Column(db.String(200))
    username = db.Column(db.String(200))
    password = db.Column(db.String(200))
    driver = db.Column(db.String(100), default='ODBC Driver 17 for SQL Server')
    is_configured = db.Column(db.Boolean, default=False)

class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password = db.Column(db.String(120), nullable=False)
    nome = db.Column(db.String(100))

@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))

def get_sql_engine():
    config = DatabaseConfig.query.first()
    if not config or not config.is_configured: return None
    try:
        params = (f"DRIVER={{{config.driver}}};SERVER={config.server};DATABASE={config.database};"
                  f"UID={config.username};PWD={config.password};Connection Timeout=15;")
        return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    except: return None

@app.route('/')
def index(): return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        u, p = request.form.get('username'), request.form.get('password')
        if u == 'admin' and p == 'admin123':
            user = User.query.filter_by(username='admin').first() or User(username='admin', password='admin123', nome='Gestor BI')
            if not user.id: db.session.add(user); db.session.commit()
            login_user(user)
            return redirect(url_for('dashboard'))
        flash('Usuário ou senha inválidos.', 'danger')
    return render_template('login.html')

@app.route('/config-db', methods=['GET', 'POST'])
def config_db():
    if request.method == 'POST':
        c = DatabaseConfig.query.first() or DatabaseConfig()
        c.server, c.database = request.form.get('server'), request.form.get('database')
        c.username, c.password = request.form.get('username'), request.form.get('password')
        c.driver, c.is_configured = request.form.get('driver'), True
        db.session.add(c); db.session.commit()
        flash('Banco configurado com sucesso!', 'success')
        return redirect(url_for('login'))
    return render_template('config_db.html')

@app.route('/logout')
def logout():
    logout_user()
    return redirect(url_for('login'))

@app.route('/dashboard')
@login_required
def dashboard():
    engine = get_sql_engine()
    cards, top_v, graficos = {'total_vendas': 0, 'total_pedidos': 0}, [], {}
    if engine:
        with engine.connect() as conn:
            df_cards = pd.read_sql(text("SELECT ISNULL(SUM(C_VlrPedido),0) as total_vendas, COUNT(Numero) as total_pedidos FROM PDVCB WHERE Cod_Estabe=0 AND Status1 IN ('P','D') AND Dat_Pedido >= DATEADD(month, DATEDIFF(month,0,GETDATE()),0)"), conn)
            if not df_cards.empty: cards = df_cards.iloc[0].to_dict()
            sql_top = "SELECT TOP 5 ve.nome_guerra, SUM(cb.C_VlrPedido) as total_vendas, COUNT(cb.Numero) as qtd_pedidos FROM PDVCB cb LEFT JOIN VENDE ve ON cb.Cod_Vendedor = ve.Codigo WHERE cb.Cod_Estabe=0 AND cb.Status1 IN ('P','D') AND cb.Dat_Pedido >= DATEADD(month, DATEDIFF(month,0,GETDATE()),0) GROUP BY ve.nome_guerra ORDER BY total_vendas DESC"
            df_top = pd.read_sql(text(sql_top), conn)
            top_v = df_top.to_dict('records')
            graficos['top_vendedores'] = json.dumps(px.bar(df_top, x='nome_guerra', y='total_vendas', title='Ranking de Faturamento'), cls=plotly.utils.PlotlyJSONEncoder)
    return render_template('dashboard.html', cards_data=cards, top_vendedores=top_v, graficos_data=graficos)

@app.route('/analise_cliente')
@login_required
def analise_cliente():
    engine = get_sql_engine()
    vendedores, ranking_mais, ranking_menos, dados_busca = [], [], [], []
    cliente_detalhe, stats_detalhe, graficos, faturas_3m = None, {}, {}, []
    recomendacoes = {'comprados': [], 'sugeridos': [], 'total_notas': 0, 'valor_total': 0, 'dias_inatividade': 0}
    fin_status = {'status': 'Sem Pendências', 'total_aberto': 0, 'total_vencido': 0, 'saldo_disponivel': 0}

    v_id = request.args.get('vendedor_id', '').strip()
    cliente_id = request.args.get('cliente_id', '').strip()
    cliente_busca = request.args.get('cliente_busca', '').strip()
    hoje = datetime.now()
    data_ini_str = request.args.get('data_inicio', hoje.replace(day=1).strftime('%Y-%m-%d'))
    data_fim_str = request.args.get('data_fim', hoje.strftime('%Y-%m-%d'))

    if engine:
        with engine.connect() as conn:
            vendedores = pd.read_sql(text("SELECT Codigo, Nome_Guerra FROM VENDE WHERE bloqueado = 0 ORDER BY Nome_Guerra"), conn).to_dict('records')
            dt_ini, dt_fim = datetime.strptime(data_ini_str, '%Y-%m-%d'), datetime.strptime(data_fim_str, '%Y-%m-%d').replace(hour=23, minute=59)

            if cliente_id:
                df_cli = pd.read_sql(text("SELECT Codigo, Razao_Social, Limite_Credito FROM clien WHERE Codigo = :cid"), conn, params={"cid": cliente_id})
                if not df_cli.empty:
                    c = df_cli.iloc[0]
                    cliente_detalhe = {'codigo': c['Codigo'], 'nome': c['Razao_Social'], 'limite': c['Limite_Credito']}

                    sql_faturas = text("SELECT TOP 3 MONTH(Dat_Emissao) as Mes, YEAR(Dat_Emissao) as Ano, SUM(Vlr_TotalNota) as Total FROM NFSCB WHERE Cod_Cliente = :cid AND Status = 'F' AND Cod_Estabe = 0 AND Dat_Emissao >= DATEADD(MONTH, -3, GETDATE()) GROUP BY YEAR(Dat_Emissao), MONTH(Dat_Emissao) ORDER BY Ano DESC, Mes DESC")
                    faturas_3m = pd.read_sql(sql_faturas, conn, params={"cid": cliente_id}).to_dict('records')

                    df_fin = pd.read_sql(text("SELECT Vlr_Saldo, DATEDIFF(Day, GETDATE(), Dat_Vencimento) as Dias FROM CTREC WHERE Cod_Cliente = :cid AND Status IN ('A','P') AND Vlr_Saldo > 0"), conn, params={"cid": cliente_id})
                    fin_status['total_aberto'] = df_fin['Vlr_Saldo'].sum() if not df_fin.empty else 0
                    fin_status['saldo_disponivel'] = (cliente_detalhe['limite'] or 0) - fin_status['total_aberto']

                    if not df_fin.empty:
                        venc = df_fin[df_fin['Dias'] < 0]
                        if not venc.empty:
                            fin_status['status'], fin_status['total_vencido'] = 'Inadimplente', venc['Vlr_Saldo'].sum()
                        else: fin_status['status'] = 'Em dia'

                    sql_rec = text("""
                        WITH ClienteProdutos AS (
                            SELECT cb.Cod_Cliente, it.Cod_Produto, p.Descricao as Produto, p.Cod_Fabricante,
                                   SUM(it.Qtd_Produto) AS QtdTotal, COUNT(DISTINCT cb.Num_Nota) AS QtdCompras
                            FROM NFSIT it
                            INNER JOIN NFSCB cb ON it.Num_Nota = cb.Num_Nota AND it.Ser_Nota = cb.Ser_Nota AND it.Cod_Estabe = cb.Cod_Estabe
                            INNER JOIN PRODU p ON it.Cod_Produto = p.Codigo
                            WHERE cb.Dat_Emissao BETWEEN :ini AND :fim AND cb.Status = 'F' AND cb.Cod_Estabe = 0
                            GROUP BY cb.Cod_Cliente, it.Cod_Produto, p.Descricao, p.Cod_Fabricante
                        ),
                        ProdutosRelacionados AS (
                            SELECT cp1.Cod_Cliente, cp2.Produto as Relacionado, cp1.Produto as Base, COUNT(DISTINCT cp2.Cod_Cliente) as Popularidade
                            FROM ClienteProdutos cp1
                            JOIN ClienteProdutos cp2 ON cp2.Cod_Cliente <> cp1.Cod_Cliente AND cp2.Cod_Fabricante = cp1.Cod_Fabricante
                            WHERE cp1.Cod_Cliente = :cid
                            GROUP BY cp1.Cod_Cliente, cp2.Produto, cp1.Produto
                        )
                        SELECT
                            (SELECT TOP 5 cp.Produto + ' (' + CAST(CAST(cp.QtdTotal AS INT) AS VARCHAR) + ' un);'
                             FROM ClienteProdutos cp WHERE cp.Cod_Cliente = :cid ORDER BY cp.QtdTotal DESC FOR XML PATH('')) as TopComprados,
                            (SELECT TOP 5 pr.Relacionado + ' (Base: ' + pr.Base + ');'
                             FROM ProdutosRelacionados pr WHERE pr.Cod_Cliente = :cid ORDER BY pr.Popularidade DESC FOR XML PATH('')) as TopSugeridos,
                            COUNT(DISTINCT Num_Nota) as Notas, SUM(Vlr_TotalNota) as Total, DATEDIFF(DAY, MAX(Dat_Emissao), GETDATE()) as Dias
                        FROM NFSCB WHERE Cod_Cliente = :cid AND Status = 'F' AND Cod_Estabe = 0 AND Dat_Emissao BETWEEN :ini AND :fim
                    """)
                    res_rec = conn.execute(sql_rec, {"cid": cliente_id, "ini": dt_ini, "fim": dt_fim}).fetchone()
                    if res_rec:
                        recomendacoes = {
                            'comprados': [x.strip() for x in (res_rec[0].split(';') if res_rec[0] else []) if x.strip()],
                            'sugeridos': [x.strip() for x in (res_rec[1].split(';') if res_rec[1] else []) if x.strip()],
                            'total_notas': res_rec[2] or 0,
                            'valor_total': res_rec[3] or 0,
                            'dias_inatividade': res_rec[4] or 0
                        }

                    # Gráfico de Evolução com Valores nas Barras - CORRIGIDO
                    sql_evolucao = text("""
                        SELECT
                            CAST(YEAR(Dat_Emissao) AS VARCHAR) + '/' + RIGHT('0' + CAST(MONTH(Dat_Emissao) AS VARCHAR), 2) as Periodo,
                            SUM(Vlr_TotalNota) as Total
                        FROM NFSCB
                        WHERE Cod_Cliente = :cid AND Status = 'F' AND Cod_Estabe = 0
                          AND Dat_Emissao BETWEEN :ini AND :fim
                        GROUP BY YEAR(Dat_Emissao), MONTH(Dat_Emissao)
                        ORDER BY Periodo
                    """)
                    df_evolucao = pd.read_sql(sql_evolucao, conn, params={"cid": cliente_id, "ini": dt_ini, "fim": dt_fim})
                    if not df_evolucao.empty:
                        # Formatar os valores para exibição
                        df_evolucao['Total_Formatado'] = df_evolucao['Total'].apply(lambda x: f"R$ {x:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.'))

                        fig = px.bar(df_evolucao, x='Periodo', y='Total', 
                                     title='Evolução Mensal de Compras (R$)',
                                     labels={'Total': 'Valor (R$)', 'Periodo': 'Mês/Ano'},
                                     color_discrete_sequence=['#28a745'])

                        # Configuração para mostrar os valores formatados dentro das barras
                        fig.update_traces(
                            texttemplate='%{text}', 
                            textposition='inside',
                            text=df_evolucao['Total_Formatado']
                        )

                        fig.update_layout(
                            height=350, 
                            margin=dict(l=20, r=20, t=40, b=20),
                            xaxis_title="Mês/Ano", 
                            yaxis_title="Faturamento",
                            uniformtext_minsize=10,
                            uniformtext_mode='hide'
                        )

                        graficos['evolucao_compras'] = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

                    df_res = pd.read_sql(text("SELECT pr.Descricao AS Produto, SUM(it.Qtd_Produto) AS Qtd FROM NFSCB cb INNER JOIN NFSIT it ON cb.Cod_Estabe = it.Cod_Estabe AND cb.Num_Nota = it.Num_Nota AND cb.Ser_Nota = it.Ser_Nota INNER JOIN PRODU pr ON it.Cod_Produto = pr.Codigo WHERE cb.Cod_Cliente = :cid AND cb.Status = 'F' AND cb.Cod_Estabe = 0 AND cb.Dat_Emissao BETWEEN :ini AND :fim GROUP BY pr.Descricao"), conn, params={"cid": cliente_id, "ini": dt_ini, "fim": dt_fim})
                    if not df_res.empty:
                        stats_detalhe['top_10_mais'] = df_res.sort_values(by='Qtd', ascending=False).head(10).to_dict('records')

            elif cliente_busca or v_id:
                p = {"ini": dt_ini, "fim": dt_fim}
                where_clauses = ["1=1"]
                if v_id:
                    where_clauses.append("ve.Codigo = :vid")
                    p["vid"] = v_id
                if cliente_busca:
                    where_clauses.append("(cl.Codigo LIKE :b OR cl.Razao_Social LIKE :b)")
                    p["b"] = f"%{cliente_busca}%"

                sql_b = text(f"SELECT TOP 50 cl.Codigo, cl.Razao_Social AS [Razao Social], ve.Nome_guerra AS [Vendedor], SUM(ISNULL(cb.Vlr_TotalNota,0)) as [Valor_Total_NF_R$] FROM clien cl LEFT JOIN enxes en ON cl.Cgc_Cpf = en.Num_CgcCpf LEFT JOIN vende ve ON en.Cod_Vendedor = ve.codigo LEFT JOIN NFSCB cb ON cb.Cod_Cliente = cl.Codigo AND cb.Status = 'F' AND cb.Cod_Estabe = 0 AND cb.Dat_Emissao BETWEEN :ini AND :fim WHERE {' AND '.join(where_clauses)} GROUP BY cl.Codigo, cl.Razao_Social, ve.Nome_guerra ORDER BY [Valor_Total_NF_R$] DESC")
                dados_busca = pd.read_sql(sql_b, conn, params=p).to_dict('records')
            else:
                sql_r = text("SELECT cl.Codigo, cl.Razao_Social AS [Razao Social], SUM(ISNULL(cb.Vlr_TotalNota,0)) as Total FROM clien cl INNER JOIN NFSCB cb ON cb.Cod_Cliente = cl.Codigo WHERE cb.Status = 'F' AND cb.Cod_Estabe = 0 AND cb.Dat_Emissao BETWEEN :ini AND :fim GROUP BY cl.Codigo, cl.Razao_Social HAVING SUM(ISNULL(cb.Vlr_TotalNota,0)) > 0")
                df_all = pd.read_sql(sql_r, conn, params={"ini": dt_ini, "fim": dt_fim})
                if not df_all.empty:
                    ranking_mais = df_all.sort_values(by='Total', ascending=False).head(10).to_dict('records')
                    ranking_menos = df_all.sort_values(by='Total', ascending=True).head(10).to_dict('records')

    return render_template('analise_cliente.html', vendedores=vendedores, ranking_mais=ranking_mais, ranking_menos=ranking_menos, dados=dados_busca,
                           cliente_detalhe=cliente_detalhe, stats_detalhe=stats_detalhe, graficos=graficos, data_inicio=data_ini_str, data_fim=data_fim_str,
                           vendedor_sel=v_id, cliente_busca=cliente_busca, financeiro=fin_status, faturas_3m=faturas_3m, recomendacoes=recomendacoes)

@app.route('/pedidos_eletronicos')
@login_required
def pedidos_eletronicos():
    s = {'atual': {'Total':{'valor':0,'qtd':0}, 'T':{'valor':0,'qtd':0}, 'M':{'valor':0,'qtd':0}},
         'anterior': {'Total':{'valor':0,'qtd':0}, 'T':{'valor':0,'qtd':0}, 'M':{'valor':0,'qtd':0}}}
    return render_template('pedidos_eletronicos.html', vendedores=[], vendedor_id='todos', data_inicio='', data_fim='', stats=s)

@app.route('/vendas_produto')
@login_required
def vendas_produto(): return render_template('vendas_produto.html', vendedores=[], stats={'atual_total':0})

@app.route('/vendas_fabricante')
@login_required
def vendas_fabricante(): return render_template('vendas_fabricante.html', vendedores=[], stats={'total_vendido':0})

if __name__ == '__main__':
    with app.app_context(): db.create_all()
    app.run(debug=True, host='0.0.0.0', port=5000)
