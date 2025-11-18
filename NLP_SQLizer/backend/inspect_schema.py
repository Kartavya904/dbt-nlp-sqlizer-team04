from sqlalchemy import create_engine, inspect

eng = create_engine('postgresql+psycopg://postgres:Kart%401710@localhost:5433/DBT_NLPSQLizer')
insp = inspect(eng)

print('Tables:', insp.get_table_names())
print('\n')

for t in insp.get_table_names():
    print(f'\n{t}:')
    for c in insp.get_columns(t):
        print(f'  - {c["name"]}: {c["type"]}')

