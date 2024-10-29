import asyncio
import aiohttp
from bs4 import BeautifulSoup
import sqlite3
import time
from lxml import etree

# Função para extrair informações do gerente usando XPath
def extract_manager_info(xpath):
    manager_name = xpath.xpath("//td[@class='maninfo']//a/text()")
    manager_name = manager_name[0].strip() if manager_name else "Desconhecido"

    manager_id_href = xpath.xpath("//td[@class='maninfo']//a/@href")
    manager_id = int(manager_id_href[0].split('/')[6]) if manager_id_href else 0

    last_active = xpath.xpath("//td[@class='maninfo']")[5].text.strip() if len(xpath.xpath("//td[@class='maninfo']")) > 5 else time.strftime('%Y-%m-%d %H:%M:%S')

    return {"manager_name": manager_name, "manager_id": manager_id, "last_active": last_active}

# Função para extrair informações rápidas do clube usando XPath
def extract_quick_facts(xpath):
    short_name = xpath.xpath("//td[@class='matches_row2_nh'][2]/text()")
    short_name = short_name[0].strip() if short_name else "Desconhecido"

    stadium = xpath.xpath("//td[@class='matches_row1_nh'][2]/text()")
    stadium = stadium[1].strip() if len(stadium) > 1 else "Desconhecido"

    rating = xpath.xpath("//td[@class='matches_row2_nh'][2]/text()")
    rating = int(rating[1].strip()) if len(rating) > 1 else 0

    league_links = xpath.xpath("//td[@class='matches_row1_nh']//a/text()")
    league_name = league_links[0].strip() if league_links else "Desconhecido"

    country = 'Desconhecido'
    countryURL = xpath.xpath("//div[@style='position: absolute; left: 194px; top: 31px; width: 76px; height: 78px; cursor: pointer;']/a/@href")
    if countryURL:
        country = countryURL[0].split('/')[6]

    league_id = 0
    league_ids = xpath.xpath("//td[@class='matches_row1_nh'][2]//a/@href")
    if league_ids:
        league_id = int(league_ids[1].split('/')[7])

    return {
        "short_name": short_name,
        "stadium": stadium,
        "rating": rating,
        "country": country,
        "league_id": league_id,
        "league_name": league_name
    }

# Função para extrair informações completas do clube
def extract_club_info(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    dom = etree.HTML(str(soup))
    team_name = soup.find('div', class_='clubname').get_text(strip=True) if soup.find('div', class_='clubname') else None

    if not team_name:
        return None

    manager_info = extract_manager_info(dom)
    quick_facts = extract_quick_facts(dom)

    return {
        "team_name": team_name,
        **quick_facts,  # Unir dicionários
        **manager_info  # Unir dicionários
    }

# Função para criar a tabela de clubes no banco de dados
def create_club_table():
    with sqlite3.connect('dugout.db') as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS clubinfo (
                id INTEGER PRIMARY KEY,
                team_name TEXT,
                short_name TEXT,
                manager_id INTEGER,
                manager_name TEXT,
                stadium TEXT,
                country TEXT,
                league_id INTEGER,
                league_name TEXT,
                rating INTEGER,
                last_active TEXT
            )
        ''')
        conn.commit()

# Função para inserir ou atualizar o banco de dados com as informações do clube
def update_club_info(club_id, club_data):
    with sqlite3.connect('dugout.db') as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO clubinfo (
                id, team_name, short_name, manager_id, manager_name, stadium, country, league_id, league_name, rating, last_active
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            club_id,
            club_data['team_name'],
            club_data['short_name'],
            club_data['manager_id'],
            club_data['manager_name'],
            club_data['stadium'],
            club_data['country'],
            club_data['league_id'],
            club_data['league_name'],
            club_data['rating'],
            club_data['last_active']
        ))
        conn.commit()

# Função assíncrona para buscar a página HTML de um clube
import aiohttp
import asyncio

async def fetch_club_page(session, club_id, session_cookie):
    url = f"https://www.dugout-online.com/clubinfo/none/clubid/{club_id}"
    cookies = {"PHPSESSID": session_cookie}
    try:
        async with session.get(url, cookies=cookies, timeout=10) as response:  # Timeout adicionado
            if response.status == 200:
                try:
                    # Lida com decodificação de caracteres e possíveis erros
                    content = await response.text(encoding='utf-8')
                    return content, club_id
                except UnicodeDecodeError:
                    # Tentativa de fallback para outra codificação
                    print(f"Erro de decodificação para o clube {club_id}. Tentando com latin-1.")
                    content = await response.text(encoding='latin-1')
                    return content, club_id
            else:
                print(f"Erro ao buscar a página do clube {club_id}: {response.status}")
                return None, club_id
    except aiohttp.ClientError as e:
        print(f"Erro na requisição do clube {club_id}: {e}")
        return None, club_id
    except asyncio.TimeoutError:
        print(f"Timeout ao buscar a página do clube {club_id}")
        return None, club_id
    except Exception as e:
        print(f"Erro inesperado na requisição do clube {club_id}: {e}")
        return None, club_id

# Função principal para processar clubes assíncronamente
async def process_clubs(start_id, end_id, session_cookie, batch_size=20):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for club_id in range(start_id, end_id + 1):
            task = fetch_club_page(session, club_id, session_cookie)
            tasks.append(task)

            if len(tasks) >= batch_size:
                await process_batch(tasks)
                tasks = []

        if tasks:
            await process_batch(tasks)

async def process_batch(tasks):
    results = await asyncio.gather(*tasks)
    for html_content, club_id in results:
        if html_content:
            club_data = extract_club_info(html_content)
            if club_data:
                update_club_info(club_id, club_data)
                print(f"Clube {club_id} atualizado com sucesso.")
            else:
                print(f"Clube {club_id} não possui informações válidas.")
        else:
            print(f"Falha ao processar clube {club_id}")

# Função para iniciar o processo
def main():
    session_cookie = ""  # Cookie
    start_id = 1  # ID inicial
    end_id = 60000  # ID final 

    create_club_table()

    try:
        asyncio.run(process_clubs(start_id, end_id, session_cookie))
    except RuntimeError as e:
        if str(e) != "Event loop is closed":
            raise

if __name__ == "__main__":
    main()
