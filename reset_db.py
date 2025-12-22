# reset_db.py
from app import app, db, DatabaseConfig, User

def reset_database():
    with app.app_context():
        # Dropar todas as tabelas
        db.drop_all()
        print("Tabelas removidas...")

        # Criar todas as tabelas novamente
        db.create_all()
        print("Tabelas criadas...")

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

        # Criar usuário admin padrão
        admin_user = User(
            username='admin',
            password='admin123',
            nome='Administrador',
            cargo='Administrador',
            is_admin=True
        )
        db.session.add(admin_user)

        db.session.commit()
        print("Banco de dados resetado com sucesso!")
        print("Configuração padrão criada.")
        print("Usuário admin criado: admin / admin123")

if __name__ == '__main__':
    reset_database()
