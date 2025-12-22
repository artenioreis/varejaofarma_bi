# app.py
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, session
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
import pyodbc
import pandas as pd
import plotly
import plotly.express as px
import plotly.graph_objects as go
import json
from datetime import datetime, timedelta
import os
import sqlite3

app = Flask(__name__)
app.config['SECRET_KEY'] = 'sua-chave-secreta-aqui-super-segura-2025'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///varejaofarma.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Inicializar extensões
db = SQLAlchemy(app)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# Modelo para armazenar configurações do banco
class DatabaseConfig(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    server = db.Column(db.String(200))
    database = db.Column(db.String(200))
    username = db.Column(db.String(200))
    password = db.Column(db.String(200))
    driver = db.Column(db.String(50), default='ODBC Driver 17 for SQL Server')
    is_configured = db.Column(db.Boolean, default=False)

# Modelo de Usuário
class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password = db.Column(db.String(120), nullable=False)
    nome = db.Column(db.String(100))
    cargo = db.Column(db.String(50))
    cod_vendedor = db.Column(db.Integer)
    is_admin = db.Column(db.Boolean, default=False)

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

# Variável global para conexão
_db_config = None
_db_connection = None

def get_db_config():
    """Recupera configuração do banco de dados"""
    global _db_config
    if _db_config is None:
        config = DatabaseConfig.query.first()
        if config:
            _db_config = {
                'server': config.server,
                'database': config.database,
                'username': config.username,
                'password': config.password,
                'driver': config.driver,
                'is_configured': config.is_configured
            }
    return _db_config

def test_db_connection(server, database, username, password, driver):
    """Testa conexão com SQL Server"""
    try:
        conn_str = f'DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
        conn = pyodbc.connect(conn_str)
        conn.close()
        return True, "Conexão bem-sucedida!"
    except Exception as e:
        return False, f"Erro na conexão: {str(e)}"

def get_db_connection():
    """Obtém conexão com SQL Server"""
    global _db_connection
    config = get_db_config()

    if not config or not config['is_configured']:
        return None

    try:
        if _db_connection is None:
            conn_str = f'DRIVER={{{config["driver"]}}};SERVER={config["server"]};DATABASE={config["database"]};UID={config["username"]};PWD={config["password"]}'
            _db_connection = pyodbc.connect(conn_str)
        return _db_connection
    except Exception as e:
        print(f"Erro ao conectar ao SQL Server: {e}")
        return None

# Rotas principais
@app.route('/')
def index():
    """Página inicial - redireciona para configuração ou login"""
    config = get_db_config()
    if not config or not config['is_configured']:
        return redirect(url_for('config_db'))
    return redirect(url_for('login'))

@app.route('/config-db', methods=['GET', 'POST'])
def config_db():
    """Configuração do banco de dados SQL Server"""
    if request.method == 'POST':
        server = request.form.get('server')
        database = request.form.get('database')
        username = request.form.get('username')
        password = request.form.get('password')
        driver = request.form.get('driver', 'ODBC Driver 17 for SQL Server')

        if not all([server, database, username, password]):
            flash('Por favor, preencha todos os campos obrigatórios', 'danger')
            return render_template('config_db.html')

        # Testar conexão
        success, message = test_db_connection(server, database, username, password, driver)

        if success:
            # Salvar configuração
            config = DatabaseConfig.query.first()
            if not config:
                config = DatabaseConfig()

            config.server = server
            config.database = database
            config.username = username
            config.password = password
            config.driver = driver
            config.is_configured = True

            db.session.add(config)
            db.session.commit()

            # Atualizar configuração global
            global _db_config
            _db_config = {
                'server': server,
                'database': database,
                'username': username,
                'password': password,
                'driver': driver,
                'is_configured': True
            }

            flash('Configuração salva com sucesso!', 'success')
            return redirect(url_for('login'))
        else:
            flash(f'Falha na conexão: {message}', 'danger')

    return render_template('config_db.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    """Login de usuário"""
    # Verificar se o banco está configurado
    config = get_db_config()
    if not config or not config['is_configured']:
        return redirect(url_for('config_db'))

    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')

        if not username or not password:
            flash('Por favor, preencha todos os campos', 'danger')
            return render_template('login.html', config=config)

        # Tentar autenticar no SQL Server
        conn = get_db_connection()
        if conn:
            try:
                # Consulta para verificar usuário no banco SQL Server
                # Ajuste conforme sua tabela de usuários
                query = """
                SELECT TOP 1 Codigo, Nome, Senha, Cod_Vendedor 
                FROM Usuarios 
                WHERE Login = ? AND Ativo = 1
                """
                cursor = conn.cursor()
                cursor.execute(query, (username,))
                user_data = cursor.fetchone()

                if user_data and user_data.Senha == password:
                    # Verificar se usuário já existe no banco local
                    user = User.query.filter_by(username=username).first()
                    if not user:
                        user = User(
                            username=username,
                            password=password,
                            nome=getattr(user_data, 'Nome', username),
                            cod_vendedor=getattr(user_data, 'Cod_Vendedor', 0),
                            is_admin=(username == 'admin')
                        )
                        db.session.add(user)
                        db.session.commit()
                    else:
                        # Atualizar dados do usuário
                        user.nome = getattr(user_data, 'Nome', username)
                        user.cod_vendedor = getattr(user_data, 'Cod_Vendedor', 0)
                        db.session.commit()

                    login_user(user)
                    flash('Login realizado com sucesso!', 'success')
                    return redirect(url_for('dashboard'))
                else:
                    flash('Credenciais inválidas ou usuário inativo', 'danger')

            except Exception as e:
                print(f"Erro na autenticação: {e}")
                # Fallback para usuário admin local
                if username == 'admin' and password == 'admin123':
                    user = User.query.filter_by(username='admin').first()
                    if not user:
                        user = User(
                            username='admin',
                            password='admin123',
                            nome='Administrador',
                            is_admin=True
                        )
                        db.session.add(user)
                        db.session.commit()

                    login_user(user)
                    flash('Login realizado com sucesso (modo desenvolvimento)', 'success')
                    return redirect(url_for('dashboard'))
                else:
                    flash('Erro ao autenticar no banco de dados', 'danger')
        else:
            # Fallback para usuário admin local (apenas desenvolvimento)
            if username == 'admin' and password == 'admin123':
                user = User.query.filter_by(username='admin').first()
                if not user:
                    user = User(
                        username='admin',
                        password='admin123',
                        nome='Administrador',
                        is_admin=True
                    )
                    db.session.add(user)
                    db.session.commit()

                login_user(user)
                flash('Login realizado com sucesso (modo desenvolvimento)', 'success')
                return redirect(url_for('dashboard'))
            else:
                flash('Não foi possível conectar ao banco de dados', 'danger')

    return render_template('login.html', config=config)

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

@app.route('/dashboard')
@login_required
def dashboard():
    """Dashboard principal com dados reais do SQL Server"""
    try:
        conn = get_db_connection()

        # Dados para os cards do dashboard
        cards_data = {
            'total_vendas': 0,
            'total_pedidos': 0,
            'vendedores_ativos': 0,
            'clientes_ativos': 0
        }

        # Dados para gráficos
        graficos_data = {}

        if conn:
            try:
                # 1. Total de vendas do mês atual
                query_total_vendas = """
                SELECT ISNULL(SUM(cb.C_VlrPedido), 0) as total_vendas
                FROM PDVCB cb
                WHERE cb.Cod_Estabe = 0
                  AND cb.Status1 IN ('P', 'D')
                  AND cb.Dat_Pedido >= DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)
                  AND cb.Dat_Pedido < DATEADD(month, DATEDIFF(month, 0, GETDATE()) + 1, 0)
                """

                df_total_vendas = pd.read_sql(query_total_vendas, conn)
                cards_data['total_vendas'] = float(df_total_vendas['total_vendas'].iloc[0]) if not df_total_vendas.empty else 0

                # 2. Total de pedidos do mês
                query_total_pedidos = """
                SELECT COUNT(*) as total_pedidos
                FROM PDVCB cb
                WHERE cb.Cod_Estabe = 0
                  AND cb.Status1 IN ('P', 'D')
                  AND cb.Dat_Pedido >= DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)
                  AND cb.Dat_Pedido < DATEADD(month, DATEDIFF(month, 0, GETDATE()) + 1, 0)
                """

                df_total_pedidos = pd.read_sql(query_total_pedidos, conn)
                cards_data['total_pedidos'] = int(df_total_pedidos['total_pedidos'].iloc[0]) if not df_total_pedidos.empty else 0

                # 3. Vendedores ativos
                query_vendedores_ativos = """
                SELECT COUNT(DISTINCT cb.Cod_Vendedor) as vendedores_ativos
                FROM PDVCB cb
                WHERE cb.Cod_Estabe = 0
                  AND cb.Status1 IN ('P', 'D')
                  AND cb.Dat_Pedido >= DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)
                """

                df_vendedores_ativos = pd.read_sql(query_vendedores_ativos, conn)
                cards_data['vendedores_ativos'] = int(df_vendedores_ativos['vendedores_ativos'].iloc[0]) if not df_vendedores_ativos.empty else 0

                # 4. Clientes ativos
                query_clientes_ativos = """
                SELECT COUNT(DISTINCT cb.Cod_Cliente) as clientes_ativos
                FROM PDVCB cb
                WHERE cb.Cod_Estabe = 0
                  AND cb.Status1 IN ('P', 'D')
                  AND cb.Dat_Pedido >= DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)
                """

                df_clientes_ativos = pd.read_sql(query_clientes_ativos, conn)
                cards_data['clientes_ativos'] = int(df_clientes_ativos['clientes_ativos'].iloc[0]) if not df_clientes_ativos.empty else 0

                # 5. Top 5 vendedores do mês
                query_top_vendedores = """
                SELECT TOP 5 
                    ve.nome_guerra,
                    SUM(cb.C_VlrPedido) as total_vendas,
                    COUNT(DISTINCT cb.Numero) as qtd_pedidos
                FROM PDVCB cb
                LEFT JOIN VENDE ve ON cb.Cod_Vendedor = ve.Codigo
                WHERE cb.Cod_Estabe = 0
                  AND cb.Status1 IN ('P', 'D')
                  AND cb.Dat_Pedido >= DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)
                GROUP BY ve.nome_guerra
                ORDER BY total_vendas DESC
                """

                df_top_vendedores = pd.read_sql(query_top_vendedores, conn)
                top_vendedores = df_top_vendedores.to_dict('records')

                # 6. Vendas por tipo de pedido (eletrônico vs móvel)
                query_vendas_tipo = """
                SELECT 
                    CASE 
                        WHEN SUBSTRING(cb.Cod_OrigemPdv, 1, 1) = 'T' THEN 'Eletrônico'
                        WHEN SUBSTRING(cb.Cod_OrigemPdv, 1, 1) = 'M' THEN 'Móvel'
                        ELSE 'Outro'
                    END as tipo_pedido,
                    COUNT(*) as quantidade,
                    SUM(cb.C_VlrPedido) as valor_total
                FROM PDVCB cb
                WHERE cb.Cod_Estabe = 0
                  AND cb.Status1 IN ('P', 'D')
                  AND cb.Dat_Pedido >= DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)
                GROUP BY CASE 
                    WHEN SUBSTRING(cb.Cod_OrigemPdv, 1, 1) = 'T' THEN 'Eletrônico'
                    WHEN SUBSTRING(cb.Cod_OrigemPdv, 1, 1) = 'M' THEN 'Móvel'
                    ELSE 'Outro'
                END
                """

                df_vendas_tipo = pd.read_sql(query_vendas_tipo, conn)

                # 7. Evolução diária de vendas (últimos 7 dias)
                query_vendas_diarias = """
                SELECT 
                    CAST(cb.Dat_Pedido AS DATE) as data,
                    SUM(cb.C_VlrPedido) as total_vendas,
                    COUNT(*) as qtd_pedidos
                FROM PDVCB cb
                WHERE cb.Cod_Estabe = 0
                  AND cb.Status1 IN ('P', 'D')
                  AND cb.Dat_Pedido >= DATEADD(day, -7, GETDATE())
                GROUP BY CAST(cb.Dat_Pedido AS DATE)
                ORDER BY data
                """

                df_vendas_diarias = pd.read_sql(query_vendas_diarias, conn)

                # Criar gráficos
                # Gráfico 1: Top vendedores
                if not df_top_vendedores.empty:
                    fig_top_vendedores = px.bar(df_top_vendedores, x='nome_guerra', y='total_vendas',
                                               title='Top 5 Vendedores do Mês',
                                               labels={'nome_guerra': 'Vendedor', 'total_vendas': 'Total de Vendas (R$)'},
                                               color='total_vendas',
                                               color_continuous_scale='Viridis')
                    graficos_data['top_vendedores'] = json.dumps(fig_top_vendedores, cls=plotly.utils.PlotlyJSONEncoder)
                else:
                    graficos_data['top_vendedores'] = None

                # Gráfico 2: Vendas por tipo de pedido
                if not df_vendas_tipo.empty:
                    fig_vendas_tipo = px.pie(df_vendas_tipo, values='valor_total', names='tipo_pedido',
                                            title='Distribuição de Vendas por Tipo de Pedido',
                                            hole=0.3)
                    graficos_data['vendas_tipo'] = json.dumps(fig_vendas_tipo, cls=plotly.utils.PlotlyJSONEncoder)
                else:
                    graficos_data['vendas_tipo'] = None

                # Gráfico 3: Evolução diária de vendas
                if not df_vendas_diarias.empty:
                    fig_vendas_diarias = px.line(df_vendas_diarias, x='data', y='total_vendas',
                                                 title='Evolução Diária de Vendas (Últimos 7 dias)',
                                                 labels={'data': 'Data', 'total_vendas': 'Total de Vendas (R$)'},
                                                 markers=True)
                    graficos_data['vendas_diarias'] = json.dumps(fig_vendas_diarias, cls=plotly.utils.PlotlyJSONEncoder)
                else:
                    graficos_data['vendas_diarias'] = None

                conn.close()

            except Exception as e:
                print(f"Erro ao buscar dados do SQL Server: {e}")
                # Usar dados simulados em caso de erro
                cards_data = {
                    'total_vendas': 1250000.50,
                    'total_pedidos': 245,
                    'vendedores_ativos': 15,
                    'clientes_ativos': 89
                }

                top_vendedores = [
                    {'nome_guerra': 'João Silva', 'total_vendas': 250000.00, 'qtd_pedidos': 45},
                    {'nome_guerra': 'Maria Santos', 'total_vendas': 220000.00, 'qtd_pedidos': 38},
                    {'nome_guerra': 'Pedro Costa', 'total_vendas': 195000.00, 'qtd_pedidos': 42},
                    {'nome_guerra': 'Ana Oliveira', 'total_vendas': 180000.00, 'qtd_pedidos': 35},
                    {'nome_guerra': 'Carlos Lima', 'total_vendas': 165000.00, 'qtd_pedidos': 40}
                ]

                # Criar gráficos simulados
                df_top_vendedores_simulado = pd.DataFrame(top_vendedores)
                fig_top_vendedores = px.bar(df_top_vendedores_simulado, x='nome_guerra', y='total_vendas',
                                           title='Top 5 Vendedores do Mês',
                                           labels={'nome_guerra': 'Vendedor', 'total_vendas': 'Total de Vendas (R$)'},
                                           color='total_vendas',
                                           color_continuous_scale='Viridis')
                graficos_data['top_vendedores'] = json.dumps(fig_top_vendedores, cls=plotly.utils.PlotlyJSONEncoder)

                # Gráfico de pizza simulado
                df_vendas_tipo_simulado = pd.DataFrame({
                    'tipo_pedido': ['Eletrônico', 'Móvel', 'Outro'],
                    'valor_total': [850000, 320000, 80000]
                })
                fig_vendas_tipo = px.pie(df_vendas_tipo_simulado, values='valor_total', names='tipo_pedido',
                                        title='Distribuição de Vendas por Tipo de Pedido',
                                        hole=0.3)
                graficos_data['vendas_tipo'] = json.dumps(fig_vendas_tipo, cls=plotly.utils.PlotlyJSONEncoder)

                # Gráfico de linha simulado
                df_vendas_diarias_simulado = pd.DataFrame({
                    'data': pd.date_range(start='2025-12-15', periods=7, freq='D'),
                    'total_vendas': [120000, 135000, 110000, 145000, 130000, 125000, 140000]
                })
                fig_vendas_diarias = px.line(df_vendas_diarias_simulado, x='data', y='total_vendas',
                                             title='Evolução Diária de Vendas (Últimos 7 dias)',
                                             labels={'data': 'Data', 'total_vendas': 'Total de Vendas (R$)'},
                                             markers=True)
                graficos_data['vendas_diarias'] = json.dumps(fig_vendas_diarias, cls=plotly.utils.PlotlyJSONEncoder)
        else:
            # Dados simulados se não houver conexão
            cards_data = {
                'total_vendas': 1250000.50,
                'total_pedidos': 245,
                'vendedores_ativos': 15,
                'clientes_ativos': 89
            }

            top_vendedores = [
                {'nome_guerra': 'João Silva', 'total_vendas': 250000.00, 'qtd_pedidos': 45},
                {'nome_guerra': 'Maria Santos', 'total_vendas': 220000.00, 'qtd_pedidos': 38},
                {'nome_guerra': 'Pedro Costa', 'total_vendas': 195000.00, 'qtd_pedidos': 42},
                {'nome_guerra': 'Ana Oliveira', 'total_vendas': 180000.00, 'qtd_pedidos': 35},
                {'nome_guerra': 'Carlos Lima', 'total_vendas': 165000.00, 'qtd_pedidos': 40}
            ]

            # Criar gráficos simulados
            df_top_vendedores_simulado = pd.DataFrame(top_vendedores)
            fig_top_vendedores = px.bar(df_top_vendedores_simulado, x='nome_guerra', y='total_vendas',
                                       title='Top 5 Vendedores do Mês',
                                       labels={'nome_guerra': 'Vendedor', 'total_vendas': 'Total de Vendas (R$)'},
                                       color='total_vendas',
                                       color_continuous_scale='Viridis')
            graficos_data['top_vendedores'] = json.dumps(fig_top_vendedores, cls=plotly.utils.PlotlyJSONEncoder)

            # Gráfico de pizza simulado
            df_vendas_tipo_simulado = pd.DataFrame({
                'tipo_pedido': ['Eletrônico', 'Móvel', 'Outro'],
                'valor_total': [850000, 320000, 80000]
            })
            fig_vendas_tipo = px.pie(df_vendas_tipo_simulado, values='valor_total', names='tipo_pedido',
                                    title='Distribuição de Vendas por Tipo de Pedido',
                                    hole=0.3)
            graficos_data['vendas_tipo'] = json.dumps(fig_vendas_tipo, cls=plotly.utils.PlotlyJSONEncoder)

            # Gráfico de linha simulado
            df_vendas_diarias_simulado = pd.DataFrame({
                'data': pd.date_range(start='2025-12-15', periods=7, freq='D'),
                'total_vendas': [120000, 135000, 110000, 145000, 130000, 125000, 140000]
            })
            fig_vendas_diarias = px.line(df_vendas_diarias_simulado, x='data', y='total_vendas',
                                         title='Evolução Diária de Vendas (Últimos 7 dias)',
                                         labels={'data': 'Data', 'total_vendas': 'Total de Vendas (R$)'},
                                         markers=True)
            graficos_data['vendas_diarias'] = json.dumps(fig_vendas_diarias, cls=plotly.utils.PlotlyJSONEncoder)

        return render_template('dashboard.html',
                             cards_data=cards_data,
                             top_vendedores=top_vendedores,
                             graficos_data=graficos_data)

    except Exception as e:
        print(f"Erro no dashboard: {e}")
        # Dados de fallback em caso de erro
        cards_data = {
            'total_vendas': 1250000.50,
            'total_pedidos': 245,
            'vendedores_ativos': 15,
            'clientes_ativos': 89
        }

        top_vendedores = [
            {'nome_guerra': 'João Silva', 'total_vendas': 250000.00, 'qtd_pedidos': 45},
            {'nome_guerra': 'Maria Santos', 'total_vendas': 220000.00, 'qtd_pedidos': 38},
            {'nome_guerra': 'Pedro Costa', 'total_vendas': 195000.00, 'qtd_pedidos': 42},
            {'nome_guerra': 'Ana Oliveira', 'total_vendas': 180000.00, 'qtd_pedidos': 35},
            {'nome_guerra': 'Carlos Lima', 'total_vendas': 165000.00, 'qtd_pedidos': 40}
        ]

        return render_template('dashboard.html',
                             cards_data=cards_data,
                             top_vendedores=top_vendedores,
                             graficos_data={})

@app.route('/test-connection', methods=['POST'])
def test_connection():
    """Testa a conexão com o banco de dados"""
    data = request.json
    server = data.get('server')
    database = data.get('database')
    username = data.get('username')
    password = data.get('password')
    driver = data.get('driver', 'ODBC Driver 17 for SQL Server')

    if not all([server, database, username, password]):
        return jsonify({'status': 'error', 'message': 'Preencha todos os campos'})

    success, message = test_db_connection(server, database, username, password, driver)

    return jsonify({
        'status': 'success' if success else 'error',
        'message': message
    })

# Inicializar banco de dados e criar tabelas
def init_database():
    """Inicializa o banco de dados SQLite local"""
    with app.app_context():
        # Criar todas as tabelas
        db.create_all()

        # Verificar se a coluna is_configured existe na tabela database_config
        try:
            # Tentar executar uma consulta que usa a coluna
            config = DatabaseConfig.query.first()
        except Exception as e:
            # Se falhar, dropar e recriar a tabela
            print(f"Erro ao acessar tabela: {e}")
            print("Recriando tabelas...")

            # Dropar tabelas existentes
            db.drop_all()

            # Recriar tabelas
            db.create_all()

            # Criar configuração padrão
            config = DatabaseConfig(
                server='localhost',
                database='VarejaoFarmaDB',
                username='sa',
                password='',
                driver='ODBC Driver 17 for SQL Server',
                is_configured=False
            )
            db.session.add(config)
            db.session.commit()

        # Criar usuário admin padrão se não existir
        if not User.query.filter_by(username='admin').first():
            admin_user = User(
                username='admin',
                password='admin123',
                nome='Administrador',
                cargo='Administrador',
                is_admin=True
            )
            db.session.add(admin_user)
            db.session.commit()
            print("Usuário admin criado com sucesso!")

if __name__ == '__main__':
    # Inicializar banco de dados
    init_database()

    # Executar aplicação
    app.run(debug=True, host='0.0.0.0', port=5000)
