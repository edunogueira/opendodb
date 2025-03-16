#!/usr/bin/env python3
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from lxml import etree
import logging
import re
from database.db import Database  # Importa a classe Database
from database.login_manager import LoginManager  # Importa a classe LoginManager

# Configuração do logger
logging.basicConfig(filename='errors.log', level=logging.ERROR,
                    format='%(asctime)s:%(levelname)s:%(message)s')

class PlayerScraper:
    def __init__(self):
        self.session_cookie = None
        self.db = Database()
        self.active_players = self.db.get_all_players_and_attributes()

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

    # Extrai informações dos jogadores da página HTML
    def extract_player_info(self, html, club_id):
        try:
            players = []
            soup = BeautifulSoup(html, 'html.parser')
            dom = etree.HTML(str(soup))
            elements = dom.xpath('//tr[contains(@class, "matches_row1") or contains(@class, "matches_row2")]')
            for element in elements:
                i = 1 if club_id == '112411' else 0
                name = element.xpath('./td[' + str(3 + i) + ']//a/text()')[0]
                if "(Loaned out)" in name:
                    continue
                position = element.xpath('./td[' + str(1 + i) + ']//div/text()')[0]
                age = element.xpath('./td[' + str(4 + i) + ']//span/text()')[0]
                nationality_img = element.xpath('./td[' + str(5 + i) + ']/img/@src')[0]
                nationality_code = nationality_img[-7:-4]
                rating = element.xpath('./td[' + str(6 + i) + ']/span/text()')[0]

                player_url = element.xpath('./td[' + str(3 + i) + ']//a/@href')[0]
                player_id_match = re.search(r'playerID/(\d+)', player_url)
                player_id = player_id_match.group(1) if player_id_match else None

                attributesArray = element.xpath('./td[' + str(2 + i) + ']//div//div//table//span/text()')
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

    # Processa as informações de um clube
    async def process_club(self, session, club_id):
        base_url = "https://www.dugout-online.com"
        urls = [
            f"{base_url}/players/none/view/first/clubid/{club_id}",
            f"{base_url}/players/none/view/youth/clubid/{club_id}"
        ]

        tasks = [self.fetch_page(session, url) for url in urls]
        results = await asyncio.gather(*tasks)

        for html_content in results:
            if html_content:
                players = self.extract_player_info(html_content, club_id)
                if players:
                    self.update_players_and_attributes(players)
                    print(f"Clube {club_id} atualizado com sucesso.")
                    logging.error(f"Clube {club_id} atualizado com sucesso.")
                else:
                    print(f"Nenhum jogador encontrado para o clube {club_id}. Pulando atualização.")
                    logging.error(f"Nenhum jogador encontrado para o clube {club_id}. Pulando atualização.")
            else:
                logging.error(f"Falha ao processar clube {club_id}. Tentando novamente...")

    # Atualiza os jogadores e atributos no banco de dados
    def update_players_and_attributes(self, players):
        player_table = 'active'
        players_data = []
        attributes_data = []

        for player_data in players:
            player_id = int(player_data['id'])
            new_attributes = player_data['attributes']

            # Verifica se o jogador já existe na memória
            if player_id in self.active_players:
                old_attributes = self.active_players[player_id]
                self.log_attribute_changes(player_id, old_attributes, new_attributes)

            # Prepara os dados para atualização
            players_data.append((
                player_id,
                player_data['club_id'],
                player_data['name'],
                player_data['position'],
                player_data['nationality'],
                player_data['age'],
                player_data['rating']
            ))
            attributes_data.append((player_id, *new_attributes))

            # Atualiza os dados em memória
            self.active_players[player_id] = new_attributes

        # Atualiza jogadores e atributos em lote
        self.db.update_players_batch(player_table, players_data)
        self.db.update_attributes_batch(player_table, attributes_data)

    # Registra alterações nos atributos no histórico
    def log_attribute_changes(self, player_id, old_attributes, new_attributes):
        for i, (old_value, new_value) in enumerate(zip(old_attributes, new_attributes)):
            if old_value != new_value:
                column_name = [
                    'Ref', 'Tck', 'Cre', 'Sht', 'Tmw', 'One', 'Mrk', 'Pas', 'Dri', 'Sp', 
                    'Hnd', 'Hea', 'Lsh', 'Psn', 'Str', 'Com', 'Crs', 'Fto', 'Agg', 'Inf', 'Ecc'
                ][i]
                self.db.log_attribute_change(player_id, column_name, old_value)

    # Processa todos os clubes em lotes
    async def process_players(self, batch_size=20):
        clubinfo = self.db.get_clubinfo()

        async with aiohttp.ClientSession() as session:
            tasks = []
            for club_id in clubinfo:
                tasks.append(self.process_club(session, club_id))

                if len(tasks) >= batch_size:
                    await asyncio.gather(*tasks)
                    tasks = []

            if tasks:
                await asyncio.gather(*tasks)

# Função para iniciar o processo
async def main():
    # Instancia o PlayerScraper
    scraper = PlayerScraper()

    try:
        # Inicializa o scraper (faz o login)
        await scraper.initialize()

        # Processa os jogadores
        await scraper.process_players()
    except Exception as e:
        print(f"Erro: {e}")

# Executa o script
if __name__ == "__main__":
    asyncio.run(main())
