#!/usr/bin/env python3
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from lxml import etree
import logging
from database.db import Database  # Importa a classe Database
from database.login_manager import LoginManager  # Importa a classe LoginManager
from datetime import datetime
import time

# Configuração do logger
logging.basicConfig(filename='errors.log', level=logging.ERROR,
                    format='%(asctime)s:%(levelname)s:%(message)s')

class TeamScraper:
    def __init__(self):
        self.session_cookie = None
        self.db = Database()

    #Faz o login e obtém o cookie PHPSESSID
    async def initialize(self):
        login_manager = LoginManager()
        self.session_cookie = await login_manager.login()
        print(f"Cookie PHPSESSID obtido: {self.session_cookie}")

    # Busca a página HTML de um clube
    async def fetch_page(self, session, url, retries=3):
        cookies = {"PHPSESSID": self.session_cookie}
        for attempt in range(retries):
            try:
                async with session.get(url, cookies=cookies, timeout=10) as response:
                    if response.status == 200:
                        return await response.text(errors="ignore")
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

    #Funçao para extrair o html da pagina
    def extract_club_info(self, html):
        try:
            soup = BeautifulSoup(html, 'html.parser')
            dom = etree.HTML(str(soup))
            team_name = soup.find('div', class_='clubname').get_text(strip=True) if soup.find('div', class_='clubname') else None

            if not team_name:
                return None

            manager_info = self.extract_manager_info(dom)
            quick_facts = self.extract_quick_facts(dom)

            return {
                "team_name": team_name,
                **quick_facts,  # Unir dicionários
                **manager_info  # Unir dicionários
            }
        except Exception as e:
            logging.error(f"Exception caught for club {team_name}: {str(e)}")
            return []
    
    #Função para extrair informações do gerente usando XPath
    def extract_manager_info(self, xpath):
        manager_name = xpath.xpath("//td[@class='maninfo']//a/text()")
        manager_name = manager_name[0].strip() if manager_name else "Desconhecido"

        manager_id_href = xpath.xpath("//td[@class='maninfo']//a/@href")
        manager_id = int(manager_id_href[0].split('/')[6]) if manager_id_href else 0

        last_active = xpath.xpath("//td[@class='maninfo']")[5].text.strip() if len(xpath.xpath("//td[@class='maninfo']")) > 5 else time.strftime('%Y-%m-%d %H:%M:%S')

        return {"manager_name": manager_name, "manager_id": manager_id, "last_active": last_active}
    
    #Função para extrair informações rápidas do clube usando XPath
    def extract_quick_facts(self, xpath):
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

    #Funçao para verificar se o clube esta ativo ou inativo
    def get_active_inactive(self, last_active_str):
        last_active = datetime.strptime(last_active_str, '%Y-%m-%d')
        return 1 if (datetime.now() - last_active).days < 50 else 0

    # Processa as informações de um clube
    async def process_club(self, session, club_id):
        url = f"https://www.dugout-online.com/clubinfo/none/clubid/{club_id}"

        tasks = [self.fetch_page(session, url)]
        results = await asyncio.gather(*tasks)
        null_count = 0  # Contador de nulls consecutivos
        change_date = datetime.now().strftime('%Y-%m-%d')

        for html_content in results:
            if html_content:
                club_extracted = self.extract_club_info(html_content)
                if not club_extracted:
                    print(f"Nenhuma informação para o clube encontrada. {club_id}. Pulando atualização.")
                    null_count += 1
                    continue
                club  = []
                if (club_extracted['last_active'] == '00-00-0000'):
                    club_extracted['last_active'] = datetime.now().strftime('%Y-%m-%d')
                status = self.get_active_inactive(club_extracted['last_active'])
                club.append((
                    club_id, 
                    club_extracted['team_name'],
                    club_extracted['short_name'],
                    club_extracted['manager_id'],
                    club_extracted['manager_name'],
                    club_extracted['stadium'],
                    club_extracted['country'],
                    club_extracted['league_id'],
                    club_extracted['league_name'],
                    club_extracted['rating'],
                    club_extracted['last_active'],
                    status
                ))
                self.db.update_club_info(club)
                
                if status == 0 and self.clubinfo[club_id] == 1:
                    self.db.log_clubinfo_change(club_id, status, change_date)
                
                print(f"Clube {club_id} atualizado com sucesso.")
                logging.error(f"Clube {club_id} atualizado com sucesso.")
            else:
                print(f"Nenhum jogador encontrado para o clube {club_id}. Pulando atualização.")
                logging.error(f"Nenhum jogador encontrado para o clube {club_id}. Pulando atualização.")

    # Processa todos os clubes em lotes
    async def process_clubs(self, batch_size=20):
        self.clubinfo = self.db.get_clubinfo()

        async with aiohttp.ClientSession() as session:
            tasks = []
            for club_id in self.clubinfo:
                tasks.append(self.process_club(session, club_id))

                if len(tasks) >= batch_size:
                    await asyncio.gather(*tasks)
                    tasks = []

            if tasks:
                await asyncio.gather(*tasks)

    def move_players(self):
        today = datetime.today().strftime('%Y-%m-%d')
        clubs = self.db.get_club_active_history(today)
        for club in clubs:
            club_id = club[0]

            players = self.db.get_player_active(club_id)

            for player in players:
                player_id, club_id, name, position, nationality, age, rating = player

                self.db.move_player(player)

    def find_missing_clubs(self):        
        # Pega os IDs existentes no banco
        existing_clubs = set(self.db.get_clubinfo())  # Transforma em conjunto (set) para facilitar a comparação

        # Define a lista esperada de IDs (ajuste o range conforme necessário)
        expected_clubs = set(range(1, max(existing_clubs) + 1))  # Supondo que os clubes são sequenciais

        # Encontra os clubes ausentes
        missing_clubs = sorted(expected_clubs - existing_clubs)  # Ordena os IDs faltantes
        club  = []
        for team_id in missing_clubs:
            club.append((
                    team_id, 
                    "null",
                    "null",
                    0,
                    "null",
                    "null",
                    "nul",
                    0,
                    "null",
                    0,
                    "2025-03-16",
                    1
                ))
        self.db.update_club_info(club)

        return missing_clubs

# Função para iniciar o processo
async def main():
    # Instancia o TeamScraper
    scraper = TeamScraper()

    try:
        # Inicializa o scraper (faz o login)
        await scraper.initialize()

        # Processa os jogadores
        await scraper.process_clubs()

        #scraper.move_players()
    except Exception as e:
        print(f"Erro: {e}")

# Executa o script
if __name__ == "__main__":
    asyncio.run(main())
