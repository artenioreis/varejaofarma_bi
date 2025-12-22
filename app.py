# app.py - Versão Final Corrigida
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from sqlalchemy import text, create_engine
import pandas as pd
import plotly
import plotly.express as px
import json
import os

app = Flask(__name__)
app.config['SECRET_KEY'] = 'sua-chave-secreta-varejao-2025'
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
    driver = db.Column(db.String(50), default='ODBC Driver 17 for SQL Server')
    is_configured = db.Column(db.Boolean, default=False)

class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password = db.Column(db.String(120), nullable=False)
    nome = db.Column(db.String(100))
    is_admin = db.Column(db.Boolean, default=False)

@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))

def get_sql_engine():
    config = DatabaseConfig.query.first()
    if not config or not config.is_configured:
        return None
    try:
        connection_url = f"mssql+pyodbc://{config.username}:{config.password}@{config.server}/{config.database}?driver={config.driver.replace(' ', '+')}"
        return create_engine(connection_url)
    except Exception:
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
            user = User.query.filter_by(username='admin').first()
            if not user:
                user = User(username='admin', password='admin123', nome='Administrador', is_admin=True)
                db.session.add(user)
                db.session.commit()
            login_user(user)
            return redirect(url_for('dashboard'))
        flash('Credenciais inválidas', 'danger')
    return render_template('login.html')

@app.route('/config-db', methods=['GET', 'POST'])
def config_db():
    if request.method == 'POST':
        server = request.form.get('server')
        database = request.form.get('database')
        username = request.form.get('username')
        password = request.form.get('password')
        driver = request.form.get('driver')

        config = DatabaseConfig.query.first() or DatabaseConfig()
        config.server, config.database = server, database
        config.username, config.password = username, password
        config.driver, config.is_configured = driver, True
        db.session.add(config)
        db.session.commit()
        flash('Configuração salva!', 'success')
        return redirect(url_for('login'))
    return render_template('config_db.html')

@app.route('/test-connection', methods=['POST'])
def test_connection():
    # Fix do erro 404 no teste de conexão
    data = request.json
    try:
        import pyodbc
        conn_str = f"DRIVER={{{data['driver']}}};SERVER={data['server']};DATABASE={data['database']};UID={data['username']};PWD={data['password']};Timeout=5"
        conn = pyodbc.connect(conn_str)
        conn.close()
        return jsonify({'status': 'success', 'message': 'Conexão bem-sucedida!'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

# --- Rotas de Business Intelligence ---

@app.route('/dashboard')
@login_required
def dashboard():
    engine = get_sql_engine()
    cards_data = {'total_vendas': 0.0, 'total_pedidos': 0, 'vendedores_ativos': 0, 'clientes_ativos': 0}
    top_vendedores = []
    graficos_data = {}

    if engine:
        with engine.connect() as conn:
            # Cards
            df_cards = pd.read_sql(text("SELECT ISNULL(SUM(C_VlrPedido), 0) as total_vendas, COUNT(Numero) as total_pedidos, COUNT(DISTINCT Cod_Vendedor) as vendedores_ativos, COUNT(DISTINCT Cod_Cliente) as clientes_ativos FROM PDVCB WITH (NOLOCK) WHERE Cod_Estabe = 0 AND Status1 IN ('P', 'D') AND Dat_Pedido >= DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)"), conn)
            if not df_cards.empty:
                cards_data = df_cards.iloc[0].to_dict()

            # Ranking - CORREÇÃO: Adicionado qtd_pedidos à consulta para evitar o UndefinedError no Jinja2
            query_top = text("""
                SELECT TOP 5 
                    ve.nome_guerra, 
                    SUM(cb.C_VlrPedido) as total_vendas, 
                    COUNT(DISTINCT cb.Numero) as qtd_pedidos 
                FROM PDVCB cb WITH (NOLOCK) 
                LEFT JOIN VENDE ve ON cb.Cod_Vendedor = ve.Codigo 
                WHERE cb.Cod_Estabe = 0 AND cb.Status1 IN ('P', 'D') 
                  AND cb.Dat_Pedido >= DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0) 
                GROUP BY ve.nome_guerra ORDER BY total_vendas DESC
            """)
            df_top = pd.read_sql(query_top, conn)
            top_vendedores = df_top.to_dict('records')
            
            fig = px.bar(df_top, x='nome_guerra', y='total_vendas', title='Vendas por Vendedor')
            graficos_data['top_vendedores'] = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

    return render_template('dashboard.html', cards_data=cards_data, top_vendedores=top_vendedores, graficos_data=graficos_data)

@app.route('/logout')
def logout():
    logout_user()
    return redirect(url_for('login'))

# Stubs para as rotas do base.html (evita BuildError)
@app.route('/vendas_fabricante')
@login_required
def vendas_fabricante():
    return redirect(url_for('dashboard'))

@app.route('/pedidos_eletronicos')
@login_required
def pedidos_eletronicos():
    return redirect(url_for('dashboard'))

@app.route('/vendas_produto')
@login_required
def vendas_produto():
    return redirect(url_for('dashboard'))

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug=True, port=5000)