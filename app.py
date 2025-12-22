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
app.config['SECRET_KEY'] = 'varejao-farma-bi-secret-2025'
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

# --- Funções Auxiliares de Conexão ---
def get_sql_engine():
    config = DatabaseConfig.query.first()
    if not config or not config.is_configured:
        return None
    try:
        # Formata a string para SQLAlchemy + pyodbc
        params = (
            f"DRIVER={{{config.driver}}};"
            f"SERVER={config.server};"
            f"DATABASE={config.database};"
            f"UID={config.username};"
            f"PWD={config.password};"
            "Connection Timeout=10;"
        )
        connection_url = f"mssql+pyodbc:///?odbc_connect={params}"
        return create_engine(connection_url)
    except Exception as e:
        print(f"Erro ao criar engine: {e}")
        return None

# --- Rotas de Autenticação e Configuração ---

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
        # Login simplificado para o exemplo (Admin Local)
        if username == 'admin' and password == 'admin123':
            user = User.query.filter_by(username='admin').first()
            if not user:
                user = User(username='admin', password='admin123', nome='Gestor Farma')
                db.session.add(user)
                db.session.commit()
            login_user(user)
            return redirect(url_for('dashboard'))
        flash('Credenciais inválidas. Use admin / admin123', 'danger')
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

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
        flash('Configuração de base de dados guardada!', 'success')
        return redirect(url_for('login'))
    return render_template('config_db.html')

@app.route('/test-connection', methods=['POST'])
def test_connection():
    data = request.json
    try:
        conn_str = f"DRIVER={{{data['driver']}}};SERVER={data['server']};DATABASE={data['database']};UID={data['username']};PWD={data['password']};Timeout=5"
        conn = pyodbc.connect(conn_str)
        conn.close()
        return jsonify({'status': 'success', 'message': 'Conexão estabelecida com sucesso!'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

# --- Rotas de Business Intelligence (BI) ---

@app.route('/dashboard')
@login_required
def dashboard():
    engine = get_sql_engine()
    cards_data = {'total_vendas': 0.0, 'total_pedidos': 0, 'vendedores_ativos': 0, 'clientes_ativos': 0}
    top_vendedores = []
    graficos_data = {}

    if engine:
        try:
            with engine.connect() as conn:
                # Consulta otimizada única para os cards (Performance Farma)
                query_cards = text("""
                    SELECT 
                        ISNULL(SUM(C_VlrPedido), 0) as total_vendas,
                        COUNT(Numero) as total_pedidos,
                        COUNT(DISTINCT Cod_Vendedor) as vendedores_ativos,
                        COUNT(DISTINCT Cod_Cliente) as clientes_ativos
                    FROM PDVCB WITH (NOLOCK)
                    WHERE Cod_Estabe = 0 AND Status1 IN ('P', 'D')
                      AND Dat_Pedido >= DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)
                """)
                df_cards = pd.read_sql(query_cards, conn)
                if not df_cards.empty:
                    cards_data = df_cards.iloc[0].to_dict()

                # Top 5 Vendedores com Ticket Médio (necessário para o HTML)
                query_top = text("""
                    SELECT TOP 5 
                        ve.nome_guerra, 
                        SUM(cb.C_VlrPedido) as total_vendas, 
                        COUNT(DISTINCT cb.Numero) as qtd_pedidos
                    FROM PDVCB cb WITH (NOLOCK)
                    LEFT JOIN VENDE ve ON cb.Cod_Vendedor = ve.Codigo
                    WHERE cb.Cod_Estabe = 0 AND cb.Status1 IN ('P', 'D')
                      AND cb.Dat_Pedido >= DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)
                    GROUP BY ve.nome_guerra
                    ORDER BY total_vendas DESC
                """)
                df_top = pd.read_sql(query_top, conn)
                top_vendedores = df_top.to_dict('records')
                
                fig = px.bar(df_top, x='nome_guerra', y='total_vendas', title='Top 5 Vendas Mensais')
                graficos_data['top_vendedores'] = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        except Exception as e:
            flash(f"Erro ao ler dados do SQL Server: {e}", "danger")

    return render_template('dashboard.html', cards_data=cards_data, top_vendedores=top_vendedores, graficos_data=graficos_data)

@app.route('/vendas_fabricante')
@login_required
def vendas_fabricante():
    engine = get_sql_engine()
    vendas = []
    if engine:
        try:
            with engine.connect() as conn:
                # INNER JOIN VECOT: Mostra apenas vendedores com metas configuradas
                query = text("""
                    SELECT 
                        x.Fantasia AS fabricante,
                        x.Nome_Guerra AS vendedor,
                        v.Qtd_Cota AS meta,
                        x.Unidades_Vendidas AS realizadas,
                        Faltam = CASE WHEN v.Qtd_Cota - x.Unidades_Vendidas > 0 THEN v.Qtd_Cota - x.Unidades_Vendidas ELSE 0 END,
                        Status = CASE WHEN x.Unidades_Vendidas >= v.Qtd_Cota THEN 'META BATIDA' ELSE 'PENDENTE' END
                    FROM (
                        SELECT 
                            YEAR(cb.Dat_Emissao) AS Ano, MONTH(cb.Dat_Emissao) AS Mes,
                            pr.Cod_Fabricante, fb.Fantasia, ve.Codigo AS CodVen, ve.Nome_Guerra,
                            SUM(it.Qtd_Produto + it.Qtd_Bonificacao) AS Unidades_Vendidas
                        FROM NFSCB cb WITH (NOLOCK)
                        INNER JOIN NFSIT it ON cb.Cod_Estabe = it.Cod_Estabe AND cb.Ser_Nota = it.Ser_Nota AND cb.Num_Nota = it.Num_Nota
                        INNER JOIN PRODU pr ON it.Cod_Produto = pr.Codigo
                        INNER JOIN FABRI fb ON pr.Cod_Fabricante = fb.Codigo
                        INNER JOIN VENDE ve ON cb.Cod_Vendedor = ve.Codigo
                        WHERE cb.Cod_Estabe = 0 AND cb.Status = 'F' AND cb.Tip_Saida = 'V'
                          AND cb.Dat_Emissao >= DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)
                        GROUP BY YEAR(cb.Dat_Emissao), MONTH(cb.Dat_Emissao), pr.Cod_Fabricante, fb.Fantasia, ve.Codigo, ve.Nome_Guerra
                    ) x
                    INNER JOIN VECOT v ON x.CodVen = v.Cod_Vendedor 
                                      AND x.Cod_Fabricante = v.Cod_Fabricante 
                                      AND x.Ano = v.Ano_Ref AND x.Mes = v.Mes_Ref
                    ORDER BY x.Nome_Guerra, x.Fantasia
                """)
                vendas = pd.read_sql(query, conn).to_dict('records')
        except Exception as e:
            flash(f"Erro no relatório de fabricantes: {e}", "danger")
            
    return render_template('vendas_fabricante.html', vendas=vendas)

@app.route('/pedidos_eletronicos')
@login_required
def pedidos_eletronicos():
    engine = get_sql_engine()
    resumo = []
    if engine:
        try:
            with engine.connect() as conn:
                # Agrupamento por Vendedor com Totais Individuais T e M
                query = text("""
                    SELECT 
                        ve.nome_guerra AS vendedor,
                        SUM(CASE WHEN cb.Cod_OrigemPdv LIKE 'T%' THEN cb.C_VlrPedido ELSE 0 END) AS total_t,
                        SUM(CASE WHEN cb.Cod_OrigemPdv LIKE 'M%' THEN cb.C_VlrPedido ELSE 0 END) AS total_m,
                        SUM(cb.C_VlrPedido) AS total_geral
                    FROM PDVCB cb WITH (NOLOCK)
                    LEFT JOIN VENDE ve ON cb.Cod_Vendedor = ve.Codigo
                    WHERE cb.Cod_Estabe = 0 AND cb.Tip_Pedido <> 'C' AND cb.Status1 IN ('P', 'D')
                      AND (cb.Cod_OrigemPdv LIKE 'T%' OR cb.Cod_OrigemPdv LIKE 'M%')
                      AND cb.Dat_Pedido >= DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)
                    GROUP BY ve.nome_guerra
                    HAVING SUM(cb.C_VlrPedido) > 0
                    ORDER BY ve.nome_guerra
                """)
                resumo = pd.read_sql(query, conn).to_dict('records')
        except Exception as e:
            flash(f"Erro no resumo de pedidos: {e}", "danger")

    return render_template('pedidos_eletronicos.html', resumo=resumo)

@app.route('/vendas_produto')
@login_required
def vendas_produto():
    engine = get_sql_engine()
    produtos = []
    if engine:
        try:
            with engine.connect() as conn:
                query = text("""
                    SELECT TOP 50 
                        ve.Nome_Guerra as vendedor, pr.Descricao as produto, 
                        SUM(it.Qtd_Produto + it.Qtd_Bonificacao) as unidades
                    FROM NFSCB cb WITH (NOLOCK)
                    INNER JOIN NFSIT it ON cb.Cod_Estabe = it.Cod_Estabe AND cb.Ser_Nota = it.Ser_Nota AND cb.Num_Nota = it.Num_Nota
                    INNER JOIN PRODU pr ON it.Cod_Produto = pr.Codigo
                    INNER JOIN VENDE ve ON cb.Cod_Vendedor = ve.Codigo
                    WHERE cb.Cod_Estabe = 0 AND cb.Status = 'F'
                      AND cb.Dat_Emissao >= DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)
                    GROUP BY ve.Nome_Guerra, pr.Descricao
                    ORDER BY unidades DESC
                """)
                produtos = pd.read_sql(query, conn).to_dict('records')
        except Exception as e:
            flash(f"Erro no relatório de produtos: {e}", "danger")
            
    return render_template('vendas_produto.html', produtos=produtos)

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    # Host 0.0.0.0 permite acesso na rede local se necessário
    app.run(debug=True, host='0.0.0.0', port=5000)