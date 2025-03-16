app_mysql.py
<br>Aplicação web para buscar no base de dados
<br>python3 -m venv myenv
<br>source myenv/bin/activate
<br>export FLASK_APP=app_mysql.py
<br>flask run --host=0.0.0.0
<br>nohup flask run --host=0.0.0.0 &
<br>gunicorn -w 4 -b 0.0.0.0:5000 app_mysql:app
<br>deactivate

<br>MissingTeams.py
<br>Faz um diff na base e adiciona times generios nos ids que estão faltando

<br>PlayerScraper.py
<br>Para extrair os jogadores dos times ativos salvos no banco de dados
<br>* Na linha 54 alterar o club_id para o seu club id
<br>* Na linha 157 se você quiser alterar clubes especificos, passar um array. Exp: get_clubinfo([1000,112411, 115000)

<br>PlayerScraperInactive.py
<br>Para extrair os jogadores dos times inativos salvos no banco de dados
<br>* Na linha 54 alterar o club_id para o seu club id

<br>TeamScraper.py
<br>Para atualizar os times salvos na base dados

<br>TO:DO
<br>Implementar método move_players, para mover clubes e jogadores da base ativa para inativa
