#!/usr/bin/env python3
from TeamScraper import TeamScraper  # Importa sua classe de scraper

def main():
    scraper = TeamScraper()
    
    # Busca os times faltantes
    missing_teams = scraper.find_missing_clubs()
    print(f"Lista de times ausentes salva em missing_teams.txt ({len(missing_teams)} times).")

if __name__ == "__main__":
    main()
