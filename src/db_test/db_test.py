import sqlite3


conn = sqlite3.connect("yahoo_finance_news.db")
cursor = conn.cursor()

cursor.execute(f'''
    SELECT name FROM sqlite_master WHERE type='table';
''')

cursor.execute(f'''
    SELECT name FROM sqlite_master WHERE type='table';
''')
tables = cursor.fetchall()
print(tables)