__pycache__/
*.pyc
.env

# Bases SQLite operativas: no subir datos reales al repositorio
*.db
*.sqlite
*.sqlite3

# Temporales de Excel
~$*.xlsx

# Ignorar datos operativos por defecto, pero conservar maestros requeridos
data/*
!data/.gitkeep
!data/maestro_sku_ean.xlsx
!data/packs.xlsx
!data/Packs.xlsx
