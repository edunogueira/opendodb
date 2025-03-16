from flask import Flask, render_template, request
import mysql.connector
from mysql.connector import Error

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/consultar', methods=['POST'])
def consultar():
    nationality = request.form.get('nationality', '').strip().lower()
    position = request.form.get('position', '').strip()
    age_str = request.form.get('age', '').strip()
    age = int(age_str) if age_str else 0
    active = request.form.get('active') == 'on'

    attributes_fields = [
        'ref', 'tck', 'cre', 'sht', 'tmw', 'one',
        'mrk', 'pas', 'dri', 'sp', 'hnd', 'hea',
        'lsh', 'psn', 'str', 'com', 'crs', 'fto',
        'agg', 'inf', 'ecc'
    ]

    attributes_values = {field: request.form.get(field, None) for field in attributes_fields}

    player_table = "player_active" if active else "player_inactive"
    attributes_table = "attributes_active" if active else "attributes_inactive"

    query = build_query(player_table, attributes_table, nationality, age, position, attributes_values)

    try:
        # Conexão ao MySQL
        mysql_conn = mysql.connector.connect(
            host="",
            user="",
            password="",
            database=""
        )

        # Executar a consulta
        with mysql_conn.cursor(dictionary=True) as cursor:
            cursor.execute(query['sql'], query['params'])
            resultados = cursor.fetchall()

    except Error as e:
        return render_template('index.html', error=f"Erro ao acessar o banco de dados MySQL: {e}")
    finally:
        if mysql_conn.is_connected():
            mysql_conn.close()

    return render_template('index.html', resultados=resultados)

def build_query(player_table, attributes_table, nationality, age, position, attributes_values):
    query = f'''
    SELECT 
        {player_table}.id,
        {player_table}.club_id,
        {player_table}.name,
        {player_table}.position,
        {player_table}.nationality,
        {player_table}.age,
        {player_table}.rating,
        (
            CASE 
                WHEN {player_table}.position = 'GK' THEN 
                    COALESCE({attributes_table}.Ref, 0) + COALESCE({attributes_table}.One, 0) + 
                    COALESCE({attributes_table}.Hnd, 0) + COALESCE({attributes_table}.Com, 0) + 
                    COALESCE({attributes_table}.Psn, 0)
                WHEN {player_table}.position = 'DC' THEN 
                    COALESCE({attributes_table}.Mrk, 0) + COALESCE({attributes_table}.Hea, 0) + 
                    COALESCE({attributes_table}.Tck, 0) + COALESCE({attributes_table}.Com, 0) + 
                    COALESCE({attributes_table}.Psn, 0)
                WHEN {player_table}.position IN ('DL', 'DR') THEN 
                    COALESCE({attributes_table}.Crs, 0) + COALESCE({attributes_table}.Mrk, 0) + 
                    COALESCE({attributes_table}.Tck, 0) + COALESCE({attributes_table}.Com, 0) + 
                    COALESCE({attributes_table}.Psn, 0)
                WHEN {player_table}.position IN ('ML', 'MR') THEN 
                    COALESCE({attributes_table}.Crs, 0) + COALESCE({attributes_table}.Fto, 0) + 
                    COALESCE({attributes_table}.Pas, 0) + COALESCE({attributes_table}.Cre, 0) + 
                    COALESCE({attributes_table}.Psn, 0)
                WHEN {player_table}.position = 'MC' THEN 
                    COALESCE({attributes_table}.Lsh, 0) + COALESCE({attributes_table}.Fto, 0) + 
                    COALESCE({attributes_table}.Pas, 0) + COALESCE({attributes_table}.Cre, 0) + 
                    COALESCE({attributes_table}.Psn, 0)
                WHEN {player_table}.position IN ('FL', 'FR') THEN 
                    COALESCE({attributes_table}.Sht, 0) + COALESCE({attributes_table}.Dri, 0) + 
                    COALESCE({attributes_table}.Fto, 0) + COALESCE({attributes_table}.Crs, 0) + 
                    COALESCE({attributes_table}.Psn, 0)
                WHEN {player_table}.position = 'FC' THEN 
                    COALESCE({attributes_table}.Sht, 0) + COALESCE({attributes_table}.Dri, 0) + 
                    COALESCE({attributes_table}.Fto, 0) + COALESCE({attributes_table}.Hea, 0) + 
                    COALESCE({attributes_table}.Psn, 0)
                ELSE 0
            END
        ) AS OPS
    FROM 
        {player_table}
    LEFT JOIN 
        {attributes_table} ON {player_table}.id = {attributes_table}.id
    '''

    conditions = []
    parameters = []

    if nationality and nationality != 'any':
        conditions.append(f"{player_table}.nationality = %s")
        parameters.append(nationality)

    if position and position != 'ANY':
        if position == 'DA':  # Defenders (any)
            conditions.append(f"({player_table}.position = %s OR {player_table}.position = %s OR {player_table}.position = %s)")
            parameters.append('DC')
            parameters.append('DR')
            parameters.append('DL')
        
        elif position == 'MA':  # Midfielders (any)
            conditions.append(f"({player_table}.position = %s OR {player_table}.position = %s OR {player_table}.position = %s)")
            parameters.append('MC')
            parameters.append('MR')
            parameters.append('ML')
        
        elif position == 'FA':  # Forwards (any)
            conditions.append(f"({player_table}.position = %s OR {player_table}.position = %s OR {player_table}.position = %s)")
            parameters.append('FC')
            parameters.append('FR')
            parameters.append('FL')
        else :
            conditions.append(f"{player_table}.position = %s")
            parameters.append(position)

    if age != 0:
        conditions.append(f"{player_table}.age <= %s")
        parameters.append(age)

    # Filtragem para os atributos com intervalos
    for field in attributes_values:
        min_value = request.form.get(f"{field}_min", None)
        max_value = request.form.get(f"{field}_max", None)

        if min_value is not None:
            conditions.append(f"{attributes_table}.{field} >= %s")
            parameters.append(min_value)

        if max_value is not None:
            conditions.append(f"{attributes_table}.{field} <= %s")
            parameters.append(max_value)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY OPS DESC LIMIT 1000"

    return {'sql': query, 'params': parameters}

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=4000)
