import asyncio
import aiohttp
from bs4 import BeautifulSoup
import sqlite3
from lxml import etree
import logging
from datetime import datetime
import re
import logging

# Configuração do logger
logging.basicConfig(filename='errors.log', level=logging.ERROR,
                    format='%(asctime)s:%(levelname)s:%(message)s')
                    
def extract_player_info(html, club_id):
    try:
        players = []
        soup = BeautifulSoup(html, 'html.parser')
        dom = etree.HTML(str(soup))
        elements = dom.xpath('//tr[contains(@class, "matches_row1") or contains(@class, "matches_row2")]')
        for element in elements:
            name = element.xpath('./td[3]//a/text()')[0]
            if "(Loaned out)" in name:
                continue
            position = element.xpath('./td[1]//div/text()')[0]
            age = element.xpath('./td[4]//span/text()')[0]
            nationality_img = element.xpath('./td[5]/img/@src')[0]
            nationality_code = nationality_img[-7:-4]
            rating = element.xpath('./td[6]/span/text()')[0]

            player_url = element.xpath('./td[3]//a/@href')[0]
            player_id_match = re.search(r'playerID/(\d+)', player_url)
            player_id = player_id_match.group(1) if player_id_match else None

            attributesArray = element.xpath('./td[2]//div//div//table//span/text()')
            attributes = [int(attr) for attr in attributesArray]

            players.append({
                'id': player_id,
                'club_id': club_id,
                'name': name,
                'position': position,
                'nationality': nationality_code,
                'age': age,
                'rating': rating,
                'attributes': attributes
            })

        return players
    except Exception as e:
        logging.error(f"Exception caught for club {club_id}: {str(e)}")
        return []

# Função para criar as tabelas no banco de dados
def create_tables(cursor):
    tables = [
        '''CREATE TABLE IF NOT EXISTS player_active (
            id INTEGER PRIMARY KEY,
            club_id INTEGER,
            name TEXT,
            position TEXT,
            nationality TEXT,
            age INTEGER,
            rating INTEGER
        )''',
        '''CREATE TABLE IF NOT EXISTS player_inactive (
            id INTEGER PRIMARY KEY,
            club_id INTEGER,
            name TEXT,
            position TEXT,
            nationality TEXT,
            age INTEGER,
            rating INTEGER
        )''',
        '''CREATE TABLE IF NOT EXISTS attributes_active (
            id INTEGER PRIMARY KEY,
            Ref INTEGER,
            Tck INTEGER,
            Cre INTEGER,
            Sht INTEGER,
            Tmw INTEGER,
            One INTEGER,
            Mrk INTEGER,
            Pas INTEGER,
            Dri INTEGER,
            Sp INTEGER,
            Hnd INTEGER,
            Hea INTEGER,
            Lsh INTEGER,
            Psn INTEGER,
            Str INTEGER,
            Com INTEGER,
            Crs INTEGER,
            Fto INTEGER,
            Agg INTEGER,
            Inf INTEGER,
            Ecc INTEGER
        )''',
        '''CREATE TABLE IF NOT EXISTS attributes_inactive (
            id INTEGER PRIMARY KEY,
            Ref INTEGER,
            Tck INTEGER,
            Cre INTEGER,
            Sht INTEGER,
            Tmw INTEGER,
            One INTEGER,
            Mrk INTEGER,
            Pas INTEGER,
            Dri INTEGER,
            Sp INTEGER,
            Hnd INTEGER,
            Hea INTEGER,
            Lsh INTEGER,
            Psn INTEGER,
            Str INTEGER,
            Com INTEGER,
            Crs INTEGER,
            Fto INTEGER,
            Agg INTEGER,
            Inf INTEGER,
            Ecc INTEGER
        )'''
    ]
    for table in tables:
        cursor.execute(table)

def get_clubinfo(cursor, start_id, end_id):
    cursor.execute('SELECT id, last_active FROM clubinfo WHERE id BETWEEN ? AND ?', (start_id, end_id))
    return cursor.fetchall()

def get_active_inactive(last_active_str):
    last_active = datetime.strptime(last_active_str, '%Y-%m-%d')
    return 'active' if (datetime.now() - last_active).days < 45 else 'inactive'

# Função para inserir ou atualizar o banco de dados com as informações do clube
def update_players_batch(cursor, table, players_data):
    cursor.executemany(f'''
        INSERT OR REPLACE INTO player_{table} (
            id, club_id, name, position, nationality, age, rating
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', players_data)


def update_attributes_batch(cursor, table, attributes_data):
    cursor.executemany(f'''
        INSERT OR REPLACE INTO attributes_{table} (
            id, Ref, Tck, Cre, Sht, Tmw, One, Mrk, Pas, Dri, Sp, Hnd, Hea, Lsh, Psn, Str, Com, Crs, Fto, Agg, Inf, Ecc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', attributes_data)

# Função assíncrona para buscar a página HTML de um clube
async def fetch_page(session, url, session_cookie, retries=3):
    cookies = {"PHPSESSID": session_cookie}
    for attempt in range(retries):
        try:
            async with session.get(url, cookies=cookies, timeout=10) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    logging.error(f"Erro ao buscar a página do clube {url}: {response.status}")
                    return None
        except asyncio.TimeoutError:
            logging.error(f"Timeout na requisição do clube {url}, tentativa {attempt + 1}")
            if attempt + 1 == retries:
                return None  # Retorna None após o número máximo de tentativas
        except Exception as e:
            logging.error(f"Erro na requisição do clube {url}: {e}")
            return None

async def process_players(start_id, end_id, session_cookie, batch_size=20):
    with sqlite3.connect('dugout.db') as conn:
        cursor = conn.cursor()
        create_tables(cursor)
        clubinfo = get_clubinfo(cursor, start_id, end_id)

        async with aiohttp.ClientSession() as session:
            tasks = []
            club_data = []
            for club_id, last_active in clubinfo:
                task = fetch_page(session, f"https://www.dugout-online.com/players/none/clubid/{club_id}", session_cookie)
                tasks.append(task)
                club_data.append((club_id, last_active))

                task = fetch_page(session, f"https://www.dugout-online.com/players/none/view/youth/clubid/{club_id}", session_cookie)
                tasks.append(task)
                club_data.append((club_id, last_active))

                if len(tasks) >= batch_size:
                    await process_batch(tasks, cursor, club_data)
                    conn.commit()  # Commit após cada batch
                    tasks = []
                    club_data = []

            if tasks:
                await process_batch(tasks, cursor, club_data)
            conn.commit()  # Commit final

async def process_batch(tasks, cursor, club_data):
    results = await asyncio.gather(*tasks)
    
    for (html_content, (club_id, last_active)) in zip(results, club_data):
        if html_content:
            players = extract_player_info(html_content, club_id)
            if not players:
                print(f"Nenhum jogador encontrado para o clube {club_id}. Pulando atualização.")
                continue

            player_table = get_active_inactive(last_active)
            players_data = []
            attributes_data = []

            for player_data in players:
                players_data.append((
                    player_data['id'],
                    player_data['club_id'],
                    player_data['name'],
                    player_data['position'],
                    player_data['nationality'],
                    player_data['age'],
                    player_data['rating']
                ))
                attributes_data.append((player_data['id'], *player_data['attributes']))

            # Atualizar jogadores e atributos em lote
            update_players_batch(cursor, player_table, players_data)
            update_attributes_batch(cursor, player_table, attributes_data)

            print(f"Clube {club_id} atualizado com sucesso.")
        else:
            logging.error(f"Falha ao processar clube {club_id}. Tentando novamente...")

# Função para iniciar o processo
def main():
    session_cookie = ""  # Cookie
    start_id = 1  # ID inicial
    end_id = 60000  # ID final

    try:
        asyncio.run(process_players(start_id, end_id, session_cookie))
    except RuntimeError as e:
        if str(e) != "Event loop is closed":
            raise

if __name__ == "__main__":
    main()
