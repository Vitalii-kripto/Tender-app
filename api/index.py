import os
import sys

# Добавляем корневую директорию проекта в путь поиска модулей,
# чтобы Python мог найти папку backend
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.main import app

# Vercel ищет переменную app в этом файле
