import os
import json
import pandas as pd
import plotly
import plotly.express as px
import plotly.graph_objects as go
import pyodbc
from datetime import datetime, timedelta
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from sqlalchemy import text, create_engine

app = Flask(__name__)
app.config['SECRET_KEY'] = 'varejao-farma-bi-2025-v-final-ranking-corrigido'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///varejaofarma.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
login_manager = LoginManager(app)
login_manager.login_view = 'login'

# --- Modelos de Banco de Dados Local (SQLite) ---
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

# --- Rotas Base ---

@app.route('/')
def index():
    return redirect(url_for('login'))

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

@app.route('/logout')
def logout():
    logout_user()
    return redirect(url_for('login'))

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

# --- Análise de Clientes Modificada (Financeiro CTREC) ---

@app.route('/analise_cliente')
@login_required
def analise_cliente():
    engine = get_sql_engine()
    vendedores, ranking_mais, ranking_menos, dados_busca = [], [], [], []
    cliente_detalhe, stats_detalhe, graficos = None, {}, {}
    fin_status = {'status': 'Sem Pendências', 'total_aberto': 0, 'total_vencido': 0, 'tem_documentos': False}

    v_id = request.args.get('vendedor_id', '').strip()
    cliente_id = request.args.get('cliente_id', '').strip()
    cliente_busca = request.args.get('cliente_busca', '').strip()
    
    hoje = datetime.now()
    primeiro_dia = hoje.replace(day=1)
    if hoje.month == 12: ultimo_dia = hoje.replace(year=hoje.year + 1, month=1, day=1) - timedelta(days=1)
    else: ultimo_dia = hoje.replace(month=hoje.month + 1, day=1) - timedelta(days=1)

    data_ini_str = request.args.get('data_inicio', primeiro_dia.strftime('%Y-%m-%d'))
    data_fim_str = request.args.get('data_fim', ultimo_dia.strftime('%Y-%m-%d'))

    if engine:
        with engine.connect() as conn:
            vendedores = pd.read_sql(text("SELECT Codigo, Nome_Guerra FROM VENDE WHERE bloqueado = 0 ORDER BY Nome_Guerra"), conn).to_dict('records')
            dt_ini = datetime.strptime(data_ini_str, '%Y-%m-%d')
            dt_fim = datetime.strptime(data_fim_str, '%Y-%m-%d').replace(hour=23, minute=59)

            if cliente_id:
                # Dados Básicos do Cliente
                df_cli = pd.read_sql(text("SELECT Codigo, Razao_Social, Limite_Credito FROM clien WHERE Codigo = :cid"), conn, params={"cid": cliente_id})
                if not df_cli.empty:
                    c = df_cli.iloc[0]
                    cliente_detalhe = {'codigo': c['Codigo'], 'nome': c['Razao_Social'], 'limite': c['Limite_Credito']}
                    
                    # --- NOVO: Lógica Financeira CTREC ---
                    sql_financeiro = text("""
                        SELECT 
                            Vlr_Saldo,
                            DATEDIFF(Day, DATEADD(Day, ISNULL(Qtd_DiaExtVct,0), GETDATE()), ISNULL(Dat_Vencimento,0)) as Dias_Atraso
                        FROM CTREC 
                        WHERE Cod_Cliente = :cid 
                        AND (Status = 'A' OR Status = 'P') 
                        AND Vlr_Saldo > 0
                    """)
                    df_fin = pd.read_sql(sql_financeiro, conn, params={"cid": cliente_id})
                    
                    if not df_fin.empty:
                        fin_status['tem_documentos'] = True
                        fin_status['total_aberto'] = df_fin['Vlr_Saldo'].sum()
                        vencidos = df_fin[df_fin['Dias_Atraso'] < 0]
                        if not vencidos.empty:
                            fin_status['status'] = 'Inadimplente'
                            fin_status['total_vencido'] = vencidos['Vlr_Saldo'].sum()
                        else:
                            fin_status['status'] = 'Em dia'

                    # Estatísticas de Produtos
                    sql_it = text("SELECT pr.Descricao AS Produto, SUM(it.Qtd_Produto) AS Qtd_Produto FROM NFSCB cb INNER JOIN NFSIT it ON cb.Cod_Estabe = it.Cod_Estabe AND cb.Num_Nota = it.Num_Nota AND cb.Ser_Nota = it.Ser_Nota INNER JOIN PRODU pr ON it.Cod_Produto = pr.Codigo WHERE cb.Cod_Cliente = :cid AND cb.Status = 'F' AND cb.Dat_Emissao BETWEEN :ini AND :fim GROUP BY pr.Descricao")
                    df_res = pd.read_sql(sql_it, conn, params={"cid": cliente_id, "ini": dt_ini, "fim": dt_fim})
                    
                    if not df_res.empty:
                        stats_detalhe['top_10_mais'] = df_res.sort_values(by='Qtd_Produto', ascending=False).head(10).to_dict('records')
                        stats_detalhe['top_10_menos'] = df_res.sort_values(by='Qtd_Produto', ascending=True).head(10).to_dict('records')
                        graficos['products'] = json.dumps(px.pie(df_res.head(5), values='Qtd_Produto', names='Produto', hole=.3).update_layout(height=300, margin=dict(l=10,r=10,t=10,b=10)), cls=plotly.utils.PlotlyJSONEncoder)

                    # Histórico
                    df_h = pd.read_sql(text("SELECT YEAR(Dat_Emissao) as Ano, MONTH(Dat_Emissao) as Mes, SUM(Vlr_TotalNota) as Total FROM NFSCB WHERE Cod_Cliente = :cid AND Status = 'F' AND Dat_Emissao >= DATEADD(month, -6, GETDATE()) GROUP BY YEAR(Dat_Emissao), MONTH(Dat_Emissao) ORDER BY Ano, Mes"), conn, params={"cid": cliente_id})
                    if not df_h.empty:
                        df_h['Periodo'] = df_h['Mes'].astype(str) + '/' + df_h['Ano'].astype(str); avg = df_h['Total'].mean()
                        df_p = pd.DataFrame([{'Periodo': f"{(datetime.now().month+i-1)%12+1}/{(datetime.now().year+(datetime.now().month+i-1)//12)} (P)", 'Total': avg} for i in range(1, 4)])
                        fig = go.Figure([go.Bar(x=df_h['Periodo'], y=df_h['Total'], name='Real'), go.Scatter(x=pd.concat([df_h['Periodo'], df_p['Periodo']]), y=pd.concat([df_h['Total'], df_p['Total']]), name='Projeção', line=dict(dash='dot', color='green'))])
                        graficos['projection'] = json.dumps(fig.update_layout(height=350, margin=dict(l=20,r=20,t=20,b=20)), cls=plotly.utils.PlotlyJSONEncoder)

            elif cliente_busca or v_id:
                f, p = [], {"ini": dt_ini, "fim": dt_fim}
                if v_id: f.append("ve.Codigo = :vid"); p["vid"] = v_id
                if cliente_busca: f.append("(cl.Codigo LIKE :b OR cl.Razao_Social LIKE :b)"); p["b"] = f"%{cliente_busca}%"
                where = " AND ".join(f) if f else "1=1"
                sql_busca = f"""
                    SELECT TOP 50 cl.Codigo, cl.Razao_Social AS [Razao Social], ve.Nome_guerra AS [Vendedor], SUM(ISNULL(cb.Vlr_TotalNota,0)) as [Valor_Total_NF_R$] 
                    FROM clien cl 
                    LEFT JOIN enxes en ON cl.Cgc_Cpf = en.Num_CgcCpf 
                    LEFT JOIN vende ve ON en.Cod_Vendedor = ve.codigo 
                    LEFT JOIN NFSCB cb ON cb.Cod_Cliente = cl.Codigo AND cb.Status = 'F' AND cb.Dat_Emissao BETWEEN :ini AND :fim 
                    WHERE {where} 
                    GROUP BY cl.Codigo, cl.Razao_Social, ve.Nome_guerra ORDER BY [Valor_Total_NF_R$] DESC
                """
                dados_busca = pd.read_sql(text(sql_busca), conn, params=p).to_dict('records')

            else:
                sql_ranking = """
                    SELECT cl.Codigo, cl.Razao_Social AS [Razao Social], ve.Nome_guerra AS [Vendedor], SUM(ISNULL(cb.Vlr_TotalNota,0)) as Total
                    FROM clien cl 
                    LEFT JOIN enxes en ON cl.Cgc_Cpf = en.Num_CgcCpf 
                    LEFT JOIN vende ve ON en.Cod_Vendedor = ve.codigo 
                    INNER JOIN NFSCB cb ON cb.Cod_Cliente = cl.Codigo 
                    WHERE cb.Status = 'F' AND cb.Dat_Emissao BETWEEN :ini AND :fim
                    GROUP BY cl.Codigo, cl.Razao_Social, ve.Nome_guerra
                    HAVING SUM(ISNULL(cb.Vlr_TotalNota,0)) > 0
                """
                df_all = pd.read_sql(text(sql_ranking), conn, params={"ini": dt_ini, "fim": dt_fim})
                if not df_all.empty:
                    ranking_mais = df_all.sort_values(by='Total', ascending=False).head(10).to_dict('records')
                    ranking_menos = df_all.sort_values(by='Total', ascending=True).head(10).to_dict('records')

    return render_template('analise_cliente.html', vendedores=vendedores, ranking_mais=ranking_mais, ranking_menos=ranking_menos, dados=dados_busca, cliente_detalhe=cliente_detalhe, stats_detalhe=stats_detalhe, graficos=graficos, data_inicio=data_ini_str, data_fim=data_fim_str, vendedor_sel=v_id, cliente_busca=cliente_busca, financeiro=fin_status)

# Mantendo as demais rotas para compatibilidade
@app.route('/pedidos_eletronicos')
@login_required
def pedidos_eletronicos(): return render_template('pedidos_eletronicos.html', vendedores=[], vendedor_id='todos', data_inicio='', data_fim='', stats={'atual': {'Total': {'valor':0,'qtd':0}}, 'anterior': {'Total': {'valor':0,'qtd':0}}})

@app.route('/vendas_produto')
@login_required
def vendas_produto(): return render_template('vendas_produto.html', vendas=[], vendedores=[], vendedor_sel='todos', data_inicio='', data_fim='', stats={'atual_total':0})

@app.route('/vendas_fabricante')
@login_required
def vendas_fabricante(): return render_template('vendas_fabricante.html', vendas=[], data_inicio='', data_fim='', stats={'total_vendido':0})

if __name__ == '__main__':
    with app.app_context(): db.create_all()
    app.run(debug=True, host='0.0.0.0', port=5000)