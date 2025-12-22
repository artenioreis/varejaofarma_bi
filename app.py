import os
import json
import pandas as pd
import plotly
import plotly.express as px
import pyodbc
from datetime import datetime
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

# --- Modelos de Base de Dados ---
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
        params = (
            f"DRIVER={{{config.driver}}};SERVER={config.server};DATABASE={config.database};"
            f"UID={config.username};PWD={config.password};Connection Timeout=15;"
        )
        return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    except Exception as e:
        print(f"Erro ao conectar: {e}")
        return None

# --- Rotas de Sistema ---

@app.route('/')
def index():
    config = DatabaseConfig.query.first()
    if not config or not config.is_configured:
        return redirect(url_for('config_db'))
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        if username == 'admin' and password == 'admin123':
            user = User.query.filter_by(username='admin').first() or User(username='admin', password='admin123', nome='Gestor BI')
            if not user.id: db.session.add(user); db.session.commit()
            login_user(user)
            return redirect(url_for('dashboard'))
        flash('Credenciais incorretas.', 'danger')
    return render_template('login.html')

@app.route('/config-db', methods=['GET', 'POST'])
def config_db():
    if request.method == 'POST':
        config = DatabaseConfig.query.first() or DatabaseConfig()
        config.server, config.database = request.form.get('server'), request.form.get('database')
        config.username, config.password = request.form.get('username'), request.form.get('password')
        config.driver, config.is_configured = request.form.get('driver'), True
        db.session.add(config); db.session.commit()
        flash('Configuração salva!', 'success')
        return redirect(url_for('login'))
    return render_template('config_db.html')

@app.route('/test-connection', methods=['POST'])
def test_connection():
    data = request.json
    try:
        conn = pyodbc.connect(f"DRIVER={{{data['driver']}}};SERVER={data['server']};DATABASE={data['database']};UID={data['username']};PWD={data['password']};Timeout=5")
        conn.close()
        return jsonify({'status': 'success', 'message': 'Conexão OK!'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/logout')
@login_required
def logout():
    logout_user(); return redirect(url_for('login'))

# --- Relatórios BI ---

@app.route('/dashboard')
@login_required
def dashboard():
    engine = get_sql_engine()
    cards_data, top_vendedores, graficos_data = {'total_vendas':0,'total_pedidos':0,'vendedores_ativos':0,'clientes_ativos':0}, [], {}
    if engine:
        with engine.connect() as conn:
            df = pd.read_sql(text("SELECT ISNULL(SUM(C_VlrPedido),0) as total_vendas, COUNT(Numero) as total_pedidos, COUNT(DISTINCT Cod_Vendedor) as vendedores_ativos, COUNT(DISTINCT Cod_Cliente) as clientes_ativos FROM PDVCB WHERE Cod_Estabe=0 AND Status1 IN ('P','D') AND Dat_Pedido >= DATEADD(month, DATEDIFF(month,0,GETDATE()),0)"), conn)
            if not df.empty: cards_data = df.iloc[0].to_dict()
            df_top = pd.read_sql(text("SELECT TOP 5 ve.nome_guerra, SUM(cb.C_VlrPedido) as total_vendas, COUNT(DISTINCT cb.Numero) as qtd_pedidos FROM PDVCB cb LEFT JOIN VENDE ve ON cb.Cod_Vendedor = ve.Codigo WHERE cb.Cod_Estabe=0 AND cb.Status1 IN ('P','D') AND cb.Dat_Pedido >= DATEADD(month, DATEDIFF(month,0,GETDATE()),0) GROUP BY ve.nome_guerra ORDER BY total_vendas DESC"), conn)
            top_vendedores = df_top.to_dict('records')
            graficos_data['top_vendedores'] = json.dumps(px.bar(df_top, x='nome_guerra', y='total_vendas', title='Faturação por Vendedor'), cls=plotly.utils.PlotlyJSONEncoder)
    return render_template('dashboard.html', cards_data=cards_data, top_vendedores=top_vendedores, graficos_data=graficos_data)

@app.route('/pedidos_eletronicos')
@login_required
def pedidos_eletronicos():
    engine = get_sql_engine()
    pedidos, vendedores = [], []
    vendedor_id = request.args.get('vendedor_id', 'todos')
    data_inicio = request.args.get('data_inicio', datetime.now().strftime('%Y-%m-01'))
    data_fim = request.args.get('data_fim', datetime.now().strftime('%Y-%m-%d'))

    if engine:
        with engine.connect() as conn:
            vendedores = pd.read_sql(text("SELECT Codigo, Nome_Guerra FROM VENDE ORDER BY Nome_Guerra"), conn).values.tolist()
            
            # CORREÇÃO: Converter strings para datetime para evitar DataError (smalldatetime)
            try:
                dt_start = datetime.strptime(data_inicio, '%Y-%m-%d')
                dt_end = datetime.strptime(data_fim, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
            except:
                dt_start = datetime.now().replace(day=1); dt_end = datetime.now()

            params = {"start": dt_start, "end": dt_end}
            where = "WHERE cb.Cod_Estabe = 0 AND cb.Dat_Pedido >= :start AND cb.Dat_Pedido <= :end"
            if vendedor_id != 'todos':
                where += " AND cb.Cod_Vendedor = :v_id"
                params["v_id"] = vendedor_id

            query = text(f"SELECT cb.Numero, cb.Dat_Pedido, cb.C_VlrPedido, ve.Nome_Guerra, cb.Status1 FROM PDVCB cb WITH (NOLOCK) LEFT JOIN VENDE ve ON cb.Cod_Vendedor = ve.Codigo {where} ORDER BY cb.Dat_Pedido DESC")
            pedidos = pd.read_sql(query, conn, params=params).values.tolist()

    return render_template('pedidos_eletronicos.html', pedidos=pedidos, vendedores=vendedores, vendedor_id=vendedor_id, data_inicio=data_inicio, data_fim=data_fim)

@app.route('/vendas_fabricante')
@login_required
def vendas_fabricante():
    engine = get_sql_engine()
    vendas = []
    if engine:
        with engine.connect() as conn:
            vendas = pd.read_sql(text("SELECT x.Fantasia AS fabricante, x.Nome_Guerra AS vendedor, v.Qtd_Cota AS meta, x.Unidades_Vendidas AS realizadas, Faltam = CASE WHEN v.Qtd_Cota - x.Unidades_Vendidas > 0 THEN v.Qtd_Cota - x.Unidades_Vendidas ELSE 0 END, Status = CASE WHEN x.Unidades_Vendidas >= v.Qtd_Cota THEN 'META BATIDA' ELSE 'PENDENTE' END FROM (SELECT YEAR(cb.Dat_Emissao) AS Ano, MONTH(cb.Dat_Emissao) AS Mes, pr.Cod_Fabricante, fb.Fantasia, ve.Codigo AS CodVen, ve.Nome_Guerra, SUM(it.Qtd_Produto + it.Qtd_Bonificacao) AS Unidades_Vendidas FROM NFSCB cb INNER JOIN NFSIT it ON cb.Cod_Estabe = it.Cod_Estabe AND cb.Ser_Nota = it.Ser_Nota AND cb.Num_Nota = it.Num_Nota INNER JOIN PRODU pr ON it.Cod_Produto = pr.Codigo INNER JOIN FABRI fb ON pr.Cod_Fabricante = fb.Codigo INNER JOIN VENDE ve ON cb.Cod_Vendedor = ve.Codigo WHERE cb.Cod_Estabe = 0 AND cb.Status = 'F' AND cb.Tip_Saida = 'V' AND cb.Dat_Emissao >= DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0) GROUP BY YEAR(cb.Dat_Emissao), MONTH(cb.Dat_Emissao), pr.Cod_Fabricante, fb.Fantasia, ve.Codigo, ve.Nome_Guerra) x INNER JOIN VECOT v ON x.CodVen = v.Cod_Vendedor AND x.Cod_Fabricante = v.Cod_Fabricante AND x.Ano = v.Ano_Ref AND x.Mes = v.Mes_Ref ORDER BY x.Nome_Guerra"), conn).to_dict('records')
    return render_template('vendas_fabricante.html', vendas=vendas)

@app.route('/vendas_produto')
@login_required
def vendas_produto():
    engine = get_sql_engine()
    vendas_pr, vendedores_lista = [], []
    vendedor_id = request.args.get('vendedor_id')
    if engine:
        with engine.connect() as conn:
            vendedores_lista = pd.read_sql(text("SELECT DISTINCT ve.Codigo, ve.Nome_Guerra FROM VENDE ve INNER JOIN VECPR c ON ve.Codigo = c.Cod_Vendedor WHERE ve.Cod_Supervisor = 2 AND c.Ano_Ref = 2025 AND c.Mes_Ref = 12 ORDER BY ve.Nome_Guerra"), conn).to_dict('records')
            filtro = "AND vendas.Cod_Vendedor = :v_id" if vendedor_id else ""
            query = text(f"SELECT vendas.Nome_Guerra AS vendedor, vendas.Cod_Produto AS cod_prod, vendas.Desc_Produto AS produto, ISNULL(cotas.Qtd_Cota, 0) AS meta, vendas.Unidades AS realizadas, Faltam = CASE WHEN ISNULL(cotas.Qtd_Cota, 0) > vendas.Unidades THEN ISNULL(cotas.Qtd_Cota, 0) - vendas.Unidades ELSE 0 END, Status = CASE WHEN vendas.Unidades >= ISNULL(cotas.Qtd_Cota, 0) AND ISNULL(cotas.Qtd_Cota, 0) > 0 THEN 'META BATIDA' ELSE 'PENDENTE' END, vendas.VlrLiq AS valor_liq FROM (SELECT ve.Nome_Guerra, ve.Codigo AS Cod_Vendedor, it.Cod_Produto, pr.Descricao AS Desc_Produto, SUM(COALESCE(it.Qtd_Produto, 0) + COALESCE(it.Qtd_Bonificacao, 0)) AS Unidades, SUM(COALESCE(it.Vlr_LiqItem, 0) - COALESCE(it.Vlr_SubsTrib, 0) - COALESCE(it.Vlr_SbtRes, 0) - COALESCE(it.Vlr_RecSbt, 0) - COALESCE(it.Vlr_SubsTribEmb, 0) - COALESCE(it.Vlr_DespRateada, 0) - COALESCE(it.Vlr_DspExt, 0)) AS VlrLiq FROM NFSCB cb INNER JOIN NFSIT it ON cb.Cod_Estabe = it.Cod_Estabe AND cb.Ser_Nota = it.Ser_Nota AND cb.Num_Nota = it.Num_Nota INNER JOIN PRODU pr ON it.Cod_Produto = pr.Codigo INNER JOIN VENDE ve ON cb.Cod_Vendedor = ve.Codigo INNER JOIN SUPER su ON ve.Cod_Supervisor = su.Codigo WHERE cb.Cod_Estabe = 0 AND su.Cod_Gerencia = 2 AND ve.Cod_Supervisor = 2 AND cb.Dat_Emissao >= '20251201' AND cb.Dat_Emissao <= '20251221 23:59:59' AND cb.Status = 'F' AND cb.Tip_Saida = 'V' GROUP BY ve.Nome_Guerra, ve.Codigo, it.Cod_Produto, pr.Descricao) vendas INNER JOIN VECPR cotas ON vendas.Cod_Vendedor = cotas.Cod_Vendedor AND vendas.Cod_Produto = cotas.Cod_Produt AND cotas.Ano_Ref = 2025 AND cotas.Mes_Ref = 12 AND cotas.Cod_Estabe = 0 WHERE 1=1 {filtro} ORDER BY valor_liq DESC")
            vendas_pr = pd.read_sql(query, conn, params={"v_id": vendedor_id}).to_dict('records')
    return render_template('vendas_produto.html', vendas=vendas_pr, vendedores=vendedores_lista, vendedor_sel=vendedor_id, periodo="01/12/2025 a 21/12/2025")

if __name__ == '__main__':
    with app.app_context(): db.create_all()
    app.run(debug=True, host='0.0.0.0', port=5000)