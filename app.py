import os
import json
import pandas as pd
import plotly
import plotly.express as px
import pyodbc
from datetime import datetime, timedelta
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from sqlalchemy import text, create_engine

app = Flask(__name__)
app.config['SECRET_KEY'] = 'varejao-farma-bi-total-final-2025'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///varejaofarma.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
login_manager = LoginManager(app)
login_manager.login_view = 'login'

# --- Modelos de Dados ---
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
    if not config or not config.is_configured:
        return None
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
            login_user(user); return redirect(url_for('dashboard'))
        flash('Credenciais incorretas.', 'danger')
    return render_template('login.html')

@app.route('/logout')
def logout():
    logout_user(); return redirect(url_for('login'))

@app.route('/config-db', methods=['GET', 'POST'])
def config_db():
    if request.method == 'POST':
        c = DatabaseConfig.query.first() or DatabaseConfig()
        c.server, c.database = request.form.get('server'), request.form.get('database')
        c.username, c.password = request.form.get('username'), request.form.get('password')
        c.driver, c.is_configured = request.form.get('driver'), True
        db.session.add(c); db.session.commit()
        return redirect(url_for('login'))
    return render_template('config_db.html')

@app.route('/dashboard')
@login_required
def dashboard():
    engine = get_sql_engine()
    cards, top_v, graficos = {'total_vendas':0,'total_pedidos':0,'vendedores_ativos':0,'clientes_ativos':0}, [], {}
    if engine:
        with engine.connect() as conn:
            df = pd.read_sql(text("SELECT ISNULL(SUM(C_VlrPedido),0) as total_vendas, COUNT(Numero) as total_pedidos FROM PDVCB WHERE Cod_Estabe=0 AND Status1 IN ('P','D') AND Dat_Pedido >= DATEADD(month, DATEDIFF(month,0,GETDATE()),0)"), conn)
            if not df.empty: cards = df.iloc[0].to_dict()
            
            df_top = pd.read_sql(text("""
                SELECT TOP 5 
                    ve.nome_guerra, 
                    SUM(cb.C_VlrPedido) as total_vendas,
                    COUNT(cb.Numero) as qtd_pedidos
                FROM PDVCB cb 
                LEFT JOIN VENDE ve ON cb.Cod_Vendedor = ve.Codigo 
                WHERE cb.Cod_Estabe=0 AND cb.Status1 IN ('P','D') 
                  AND cb.Dat_Pedido >= DATEADD(month, DATEDIFF(month,0,GETDATE()),0) 
                GROUP BY ve.nome_guerra 
                ORDER BY total_vendas DESC
            """), conn)
            
            top_v = df_top.to_dict('records')
            graficos['top_vendedores'] = json.dumps(px.bar(df_top, x='nome_guerra', y='total_vendas', title='Faturamento por Vendedor'), cls=plotly.utils.PlotlyJSONEncoder)
            
    return render_template('dashboard.html', cards_data=cards, top_vendedores=top_v, graficos_data=graficos)

@app.route('/vendas_fabricante')
@login_required
def vendas_fabricante():
    engine = get_sql_engine()
    vendas_list, vendedores_meta = [], []
    stats = {'total_vendido': 0, 'total_meta': 0}
    
    v_id = request.args.get('vendedor_id', 'todos')
    data_ini = request.args.get('data_inicio', datetime.now().strftime('%Y-%m-01'))
    data_fim = request.args.get('data_fim', datetime.now().strftime('%Y-%m-%d'))

    if engine:
        with engine.connect() as conn:
            dt_ref = datetime.strptime(data_ini, '%Y-%m-%d')
            
            # Puxa vendedoras que estão na VECOT para o filtro
            v_sql = """
                SELECT DISTINCT ve.Codigo, ve.Nome_Guerra 
                FROM VENDE ve 
                INNER JOIN VECOT v ON ve.Codigo = v.Cod_Vendedor 
                WHERE v.Ano_Ref = :a AND v.Mes_Ref = :m 
                ORDER BY ve.Nome_Guerra
            """
            v_params_list = {"a": dt_ref.year, "m": dt_ref.month}
            vendedores_meta = pd.read_sql(text(v_sql), conn, params=v_params_list).to_dict('records')

            filtro_v = "AND ve.Codigo = :vid" if v_id != 'todos' else ""
            
            main_sql = f"""
            SELECT 
                x.Nome_Guerra, x.Fantasia,
                Qtd_Cota_Mensal = ISNULL(v.Qtd_Cota, 0),
                x.Unidades_Vendidas,
                Faltam = CASE WHEN ISNULL(v.Qtd_Cota, 0) - x.Unidades_Vendidas > 0 THEN ISNULL(v.Qtd_Cota, 0) - x.Unidades_Vendidas ELSE 0 END,
                Status = CASE WHEN x.Unidades_Vendidas >= ISNULL(v.Qtd_Cota, 0) AND ISNULL(v.Qtd_Cota, 0) > 0 THEN 'META BATIDA' ELSE 'PENDENTE' END
            FROM
              (SELECT 
                  YEAR(cb.Dat_Emissao) AS Ano, MONTH(cb.Dat_Emissao) AS Mes,
                  pr.Cod_Fabricante, fb.Fantasia, ve.Codigo AS CodVen, ve.Nome_Guerra,
                  Unidades_Vendidas = SUM(it.Qtd_Produto + it.Qtd_Bonificacao)
               FROM NFSCB cb
               INNER JOIN NFSIT it ON cb.Cod_Estabe = it.Cod_Estabe AND cb.Ser_Nota = it.Ser_Nota AND cb.Num_Nota = it.Num_Nota
               INNER JOIN PRODU pr ON it.Cod_Produto = pr.Codigo
               INNER JOIN FABRI fb ON pr.Cod_Fabricante = fb.Codigo
               INNER JOIN VENDE ve ON cb.Cod_Vendedor = ve.Codigo
               INNER JOIN SUPER su ON ve.Cod_Supervisor = su.Codigo
               WHERE cb.Cod_Estabe = 0 AND su.Cod_Gerencia = 2 AND ve.Cod_Supervisor = 2
                 AND cb.Dat_Emissao >= :ini AND cb.Dat_Emissao <= :fim
                 AND cb.Status = 'F' AND cb.Tip_Saida = 'V'
                 {filtro_v}
               GROUP BY YEAR(cb.Dat_Emissao), MONTH(cb.Dat_Emissao), pr.Cod_Fabricante, fb.Fantasia, ve.Codigo, ve.Nome_Guerra) x
            INNER JOIN VECOT v ON x.CodVen = v.Cod_Vendedor AND x.Cod_Fabricante = v.Cod_Fabricante AND x.Ano = v.Ano_Ref AND x.Mes = v.Mes_Ref
            ORDER BY x.Nome_Guerra, x.Fantasia
            """
            p_params = {"ini": data_ini.replace("-", ""), "fim": data_fim.replace("-", ""), "vid": v_id}
            df = pd.read_sql(text(main_sql), conn, params=p_params)
            vendas_list = df.to_dict('records')
            
            stats['total_vendido'] = int(df['Unidades_Vendidas'].sum()) if not df.empty else 0
            stats['total_meta'] = int(df['Qtd_Cota_Mensal'].sum()) if not df.empty else 0

    return render_template('vendas_fabricante.html', 
                           vendas=vendas_list, 
                           vendedores=vendedores_meta, 
                           vendedor_sel=v_id, 
                           stats=stats, 
                           data_inicio=data_ini, 
                           data_fim=data_fim)

@app.route('/vendas_produto')
@login_required
def vendas_produto():
    engine = get_sql_engine()
    vendas_pr, vendedores_lista = [], []
    stats_p = {'atual_total': 0.0, 'qtd_atual': 0, 'meta_total': 0.0}
    v_id = request.args.get('vendedor_id', 'todos')
    data_ini = request.args.get('data_inicio', datetime.now().strftime('%Y-%m-01'))
    data_fim = request.args.get('data_fim', datetime.now().strftime('%Y-%m-%d'))
    if engine:
        with engine.connect() as conn:
            dt_s = datetime.strptime(data_ini, '%Y-%m-%d')
            dt_e = datetime.strptime(data_fim, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
            v_query = "SELECT DISTINCT ve.Codigo, ve.Nome_Guerra FROM VENDE ve INNER JOIN VECPR c ON ve.Codigo = c.Cod_Vendedor WHERE c.Ano_Ref = :a AND c.Mes_Ref = :m ORDER BY ve.Nome_Guerra"
            vendedores_lista = pd.read_sql(text(v_query), conn, params={"a":dt_s.year, "m":dt_s.month}).to_dict('records')
            filtro_v = "AND ve.Codigo = :vid" if v_id != 'todos' else ""
            main_sql = f"""
                SELECT vendas.Nome_Guerra, vendas.Cod_Produto, vendas.produto, Qtd_Cota_Mensal = ISNULL(cotas.Qtd_Cota, 0), Unidades_Vendidas = vendas.Unidades, Faltam = CASE WHEN ISNULL(cotas.Qtd_Cota, 0) > vendas.Unidades THEN ISNULL(cotas.Qtd_Cota, 0) - vendas.Unidades ELSE 0 END, Status = CASE WHEN vendas.Unidades >= ISNULL(cotas.Qtd_Cota, 0) AND ISNULL(cotas.Qtd_Cota, 0) > 0 THEN 'META BATIDA' ELSE 'PENDENTE' END, vendas.VlrLiq
                FROM (SELECT ve.Nome_Guerra, ve.Codigo AS Cod_Vendedor, it.Cod_Produto, pr.Descricao as produto, Unidades = SUM(COALESCE(it.Qtd_Produto, 0) + COALESCE(it.Qtd_Bonificacao, 0)), VlrLiq = SUM(COALESCE(it.Vlr_LiqItem, 0) - COALESCE(it.Vlr_SubsTrib, 0) - COALESCE(it.Vlr_SbtRes, 0) - COALESCE(it.Vlr_RecSbt, 0) - COALESCE(it.Vlr_SubsTribEmb, 0) - COALESCE(it.Vlr_DespRateada, 0) - COALESCE(it.Vlr_DspExt, 0)) FROM NFSCB cb INNER JOIN NFSIT it ON cb.Cod_Estabe = it.Cod_Estabe AND cb.Ser_Nota = it.Ser_Nota AND cb.Num_Nota = it.Num_Nota INNER JOIN PRODU pr ON it.Cod_Produto = pr.Codigo INNER JOIN VENDE ve ON cb.Cod_Vendedor = ve.Codigo INNER JOIN SUPER su ON ve.Cod_Supervisor = su.Codigo WHERE cb.Cod_Estabe = 0 AND su.Cod_Gerencia = 2 AND ve.Cod_Supervisor = 2 AND cb.Dat_Emissao >= :s AND cb.Dat_Emissao <= :e AND cb.Status = 'F' AND cb.Tip_Saida = 'V' {filtro_v} GROUP BY ve.Nome_Guerra, ve.Codigo, it.Cod_Produto, pr.Descricao) vendas INNER JOIN VECPR cotas ON vendas.Cod_Vendedor = cotas.Cod_Vendedor AND vendas.Cod_Produto = cotas.Cod_Produt AND cotas.Ano_Ref = :a AND cotas.Mes_Ref = :m AND cotas.Cod_Estabe = 0 ORDER BY vendas.Nome_Guerra, vendas.VlrLiq DESC
            """
            p_params = {"s": dt_s, "e": dt_e, "a": dt_s.year, "m": dt_s.month}
            if v_id != 'todos': p_params["vid"] = v_id
            df = pd.read_sql(text(main_sql), conn, params=p_params)
            vendas_pr = df.to_dict('records')
            stats_p = {'atual_total': float(df['VlrLiq'].sum()), 'qtd_atual': int(df['Unidades_Vendidas'].sum()), 'meta_total': int(df['Qtd_Cota_Mensal'].sum())}
    return render_template('vendas_produto.html', vendas=vendas_pr, vendedores=vendedores_lista, vendedor_sel=v_id, stats=stats_p, data_inicio=data_ini, data_fim=data_fim)

@app.route('/pedidos_eletronicos')
@login_required
def pedidos_eletronicos():
    engine = get_sql_engine()
    vendedores, stats = [], {'atual': {'T': {'valor':0,'qtd':0}, 'M': {'valor':0,'qtd':0}, 'Total': {'valor':0,'qtd':0}}, 'anterior': {'T': {'valor':0,'qtd':0}, 'M': {'valor':0,'qtd':0}, 'Total': {'valor':0,'qtd':0}}}
    v_id = request.args.get('vendedor_id', 'todos')
    data_ini = request.args.get('data_inicio', datetime.now().strftime('%Y-%m-01'))
    data_fim = request.args.get('data_fim', datetime.now().strftime('%Y-%m-%d'))
    if engine:
        with engine.connect() as conn:
            dt_s = datetime.strptime(data_ini, '%Y-%m-%d')
            dt_e = datetime.strptime(data_fim, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
            dt_s_ant = (dt_s - timedelta(days=31)).replace(day=dt_s.day)
            dt_e_ant = dt_e - timedelta(days=31)
            v_query = "SELECT DISTINCT ve.Codigo, ve.Nome_Guerra FROM VENDE ve INNER JOIN PDVCB cb ON ve.Codigo = cb.Cod_Vendedor WHERE cb.Cod_Estabe=0 AND cb.Status1 IN ('P','D') AND cb.Dat_Pedido >= :s AND cb.Dat_Pedido <= :e ORDER BY ve.Nome_Guerra"
            vendedores = pd.read_sql(text(v_query), conn, params={"s":dt_s, "e":dt_e}).values.tolist()
            def get_stats(s, e, vid):
                where = "WHERE cb.Cod_Estabe=0 AND cb.Tip_Pedido<>'C' AND cb.Status1 IN ('P','D') AND cb.Dat_Pedido>=:s AND cb.Dat_Pedido<=:e"
                p = {"s":s, "e":e}
                if vid != 'todos': where += " AND cb.Cod_Vendedor=:vid"; p["vid"]=vid
                sql = f"SELECT SUBSTRING(cb.Cod_OrigemPdv,1,1) as Tipo, SUM(cb.C_VlrPedido) as Valor, COUNT(*) as Qtd FROM PDVCB cb {where} GROUP BY SUBSTRING(cb.Cod_OrigemPdv,1,1)"
                return pd.read_sql(text(sql), conn, params=p)
            df_at = get_stats(dt_s, dt_e, v_id); df_an = get_stats(dt_s_ant, dt_e_ant, v_id)
            for _, r in df_at.iterrows():
                if r['Tipo'] in ['T','M']: stats['atual'][r['Tipo']] = {'valor':float(r['Valor']), 'qtd':int(r['Qtd'])}
            for _, r in df_an.iterrows():
                if r['Tipo'] in ['T','M']: stats['anterior'][r['Tipo']] = {'valor':float(r['Valor']), 'qtd':int(r['Qtd'])}
            stats['atual']['Total'] = {'valor': stats['atual']['T']['valor'] + stats['atual']['M']['valor'], 'qtd': stats['atual']['T']['qtd'] + stats['atual']['M']['qtd']}
            stats['anterior']['Total'] = {'valor': stats['anterior']['T']['valor'] + stats['anterior']['M']['valor'], 'qtd': stats['anterior']['T']['qtd'] + stats['anterior']['M']['qtd']}
    return render_template('pedidos_eletronicos.html', vendedores=vendedores, vendedor_id=v_id, data_inicio=data_ini, data_fim=data_fim, stats=stats)

if __name__ == '__main__':
    with app.app_context(): db.create_all()
    app.run(debug=True, host='0.0.0.0', port=5000)