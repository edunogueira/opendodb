import os
from flask import Flask, render_template, request
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error

# Load environment variables
load_dotenv()

app = Flask(__name__)

VALID_POSITIONS = {
    'GK', 'DC', 'DL', 'DR', 'ML', 'MR', 'MC', 'FL', 'FR', 'FC',
    'DA', 'MA', 'FA', 'ANY'
}

VALID_ATTRIBUTES = {
    'ref', 'tck', 'cre', 'sht', 'tmw', 'one', 'mrk', 'pas', 'dri',
    'sp', 'hnd', 'hea', 'lsh', 'psn', 'str', 'com', 'crs', 'fto',
    'agg', 'inf', 'ecc'
}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/consultar', methods=['POST'])
def consultar():
    nationality = request.form.get('nationality', '').strip().lower()
    position = request.form.get('position', '').strip().upper()
    age_str = request.form.get('age', '').strip()
    age = int(age_str) if age_str.isdigit() else 0
    active = request.form.get('active') == 'on'

    player_table = "player_active" if active else "player_inactive"
    attributes_table = "attributes_active" if active else "attributes_inactive"

    # Atributos válidos apenas
    attributes_values = {}
    for attr in VALID_ATTRIBUTES:
        if attr + '_min' in request.form and request.form[attr + '_min'] != '1':
            attributes_values[attr] = True  # presença do checkbox
        if attr + '_max' in request.form and request.form[attr + '_max'] != '50':
            attributes_values[attr] = True  # presença do checkbox

    query_data = build_query(player_table, attributes_table, nationality, age, position, attributes_values)

    try:
        mysql_conn = mysql.connector.connect(
            host=os.environ.get("MYSQL_HOST"),
            user=os.environ.get("MYSQL_USER"),
            password=os.environ.get("MYSQL_PASSWORD"),
            database=os.environ.get("MYSQL_DB")
        )

        with mysql_conn.cursor(dictionary=True) as cursor:
            cursor.execute(query_data['sql'], query_data['params'])
            resultados = cursor.fetchall()

    except Error as e:
        return render_template('index.html', error=f"Erro ao acessar o banco de dados: {e}")
    finally:
        if mysql_conn.is_connected():
            mysql_conn.close()

    return render_template('index.html', resultados=resultados)

def build_query(player_table, attributes_table, nationality, age, position, attributes_values):
    query = f"""
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
    """

    conditions = []
    params = []

    if nationality and nationality != 'any':
        conditions.append(f"{player_table}.nationality = %s")
        params.append(nationality)

    if position in VALID_POSITIONS and position != 'ANY':
        if position == 'DA':
            conditions.append(f"{player_table}.position IN (%s, %s, %s)")
            params.extend(['DC', 'DL', 'DR'])
        elif position == 'MA':
            conditions.append(f"{player_table}.position IN (%s, %s, %s)")
            params.extend(['MC', 'ML', 'MR'])
        elif position == 'FA':
            conditions.append(f"{player_table}.position IN (%s, %s, %s)")
            params.extend(['FC', 'FL', 'FR'])
        else:
            conditions.append(f"{player_table}.position = %s")
            params.append(position)

    if age > 0:
        conditions.append(f"{player_table}.age <= %s")
        params.append(age)

    for field in attributes_values:
        try:
            min_val = int(request.form.get(f"{field}_min", ""))
            conditions.append(f"{attributes_table}.{field} >= %s")
            params.append(min_val)
        except ValueError:
            pass
        try:
            max_val = int(request.form.get(f"{field}_max", ""))
            conditions.append(f"{attributes_table}.{field} <= %s")
            params.append(max_val)
        except ValueError:
            pass

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ORDER BY OPS DESC LIMIT 1000"

    return {'sql': query, 'params': params}

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=4000)
