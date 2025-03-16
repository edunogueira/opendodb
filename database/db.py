import mysql.connector

class Database:
    def __init__(self):
        # Configurações de conexão com o banco de dados
        self.host = ""
        self.user = ""
        self.password = ""
        self.database = ""
        self.connection = None

    # Conecta ao banco de dados
    def connect(self):
        self.connection = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )
        return self.connection.cursor()

    # Fecha a conexão com o banco de dados
    def disconnect(self):
        if self.connection:
            self.connection.close()

    # Função genérica para executar queries (simples ou em lote)
    def execute_query(self, query, params=None, many=False, fetch=True):
        cursor = self.connect()
        try:
            if many:
                cursor.executemany(query, params)
            else:
                cursor.execute(query, params)

            result = cursor.fetchall() if fetch else None
            self.connection.commit()
            return result
        finally:
            self.disconnect()

    # Registra uma alteração na tabela de histórico
    def log_attribute_change(self, player_id, column_name, old_value):
        query = """
        INSERT INTO attributes_history (player_id, column_name, old_value)
        VALUES (%s, %s, %s);
        """
        self.execute_query(query, (player_id, column_name, old_value), fetch=False)

    # Função para inserir ou atualizar jogadores em lote
    def update_players_batch(self, table, players_data):
            converted_data = [
                (int(player[0]),  # id
                int(player[1]),  # club_id
                str(player[2]),  # name
                str(player[3]),  # position
                str(player[4]),  # nationality
                int(player[5]),  # age
                float(player[6])) # rating
                for player in players_data
            ]

            query = f"""
            INSERT INTO player_{table} (
                id, club_id, name, position, nationality, age, rating
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                club_id = VALUES(club_id),
                name = VALUES(name),
                position = VALUES(position),
                nationality = VALUES(nationality),
                age = VALUES(age),
                rating = VALUES(rating);
            """
            self.execute_query(query, converted_data, many=True, fetch=False)

    # Função para inserir ou atualizar atributos em lote
    def update_attributes_batch(self, table, attributes_data):
        converted_data = [
            (int(attribute[0]),  # id
            str(attribute[1]),   # Ref
            int(attribute[2]),   # Tck
            int(attribute[3]),   # Cre
            int(attribute[4]),   # Sht
            int(attribute[5]),   # Tmw
            int(attribute[6]),   # One
            int(attribute[7]),   # Mrk
            int(attribute[8]),   # Pas
            int(attribute[9]),   # Dri
            int(attribute[10]),  # Sp
            int(attribute[11]),  # Hnd
            int(attribute[12]),  # Hea
            int(attribute[13]),  # Lsh
            int(attribute[14]),  # Psn
            int(attribute[15]),  # Str
            int(attribute[16]),  # Com
            int(attribute[17]),  # Crs
            int(attribute[18]),  # Fto
            int(attribute[19]),  # Agg
            int(attribute[20]),  # Inf
            int(attribute[21])   # Ecc
            )
            for attribute in attributes_data
        ]

        query = f"""
        INSERT INTO attributes_{table} (
            id, Ref, Tck, Cre, Sht, Tmw, One, Mrk, Pas, Dri, Sp, Hnd, Hea, Lsh, Psn, Str, Com, Crs, Fto, Agg, Inf, Ecc
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            Ref = VALUES(Ref),
            Tck = VALUES(Tck),
            Cre = VALUES(Cre),
            Sht = VALUES(Sht),
            Tmw = VALUES(Tmw),
            One = VALUES(One),
            Mrk = VALUES(Mrk),
            Pas = VALUES(Pas),
            Dri = VALUES(Dri),
            Sp = VALUES(Sp),
            Hnd = VALUES(Hnd),
            Hea = VALUES(Hea),
            Lsh = VALUES(Lsh),
            Psn = VALUES(Psn),
            Str = VALUES(Str),
            Com = VALUES(Com),
            Crs = VALUES(Crs),
            Fto = VALUES(Fto),
            Agg = VALUES(Agg),
            Inf = VALUES(Inf),
            Ecc = VALUES(Ecc);
        """
        self.execute_query(query, converted_data, many=True, fetch=False)

    # Função para selecionar clubes ativos
    def get_clubinfo(self, clubs=None):
        if clubs:
            placeholders = ','.join(['%s'] * len(clubs))
            query = f"SELECT id, is_active FROM clubinfo WHERE id IN ({placeholders}) AND is_active = 1"
            params = clubs
        else:
            query = "SELECT id, is_active FROM clubinfo WHERE is_active = 1"
            params = None

        return [row[0] for row in self.execute_query(query, params)]

    def get_clubinfo_with_is_inactive(self, start=None, end=None):
        query = f"SELECT id, is_active FROM clubinfo WHERE is_active = 0 AND id >= %s AND id <= %s"
        params = [start, end]

        clubs = self.execute_query(query, params)
        return {club[0]: club[1] for club in clubs}

    # Função para atualizar informações dos clubes em lote
    def update_club_info(self, club_data):
        converted_data = [
            (int(club[0]),             # id
            str(club[1]),            # team_name
            str(club[2]),            # short_name
            int(club[3]),            # manager_id
            str(club[4]),            # manager_name
            str(club[5]),            # stadium
            str(club[6]),            # country
            int(club[7]),            # league_id
            str(club[8]),            # league_name
            float(club[9]),          # rating
            str(club[10]),           # last_active
            bool(club[11])           # is_active
            )
            for club in club_data
        ]

        query = """
        INSERT INTO clubinfo (
            id, team_name, short_name, manager_id, manager_name, stadium, country, league_id, league_name, rating, last_active, is_active
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            team_name = VALUES(team_name),
            short_name = VALUES(short_name),
            manager_id = VALUES(manager_id),
            manager_name = VALUES(manager_name),
            stadium = VALUES(stadium),
            country = VALUES(country),
            league_id = VALUES(league_id),
            league_name = VALUES(league_name),
            rating = VALUES(rating),
            last_active = VALUES(last_active),
            is_active = VALUES(is_active);
        """
        self.execute_query(query, converted_data, many=True, fetch=False)


    # Recupera todos os jogadores e atributos em um dicionário
    def get_all_players_and_attributes(self):
        query = """
        SELECT id, Ref, Tck, Cre, Sht, Tmw, One, Mrk, Pas, Dri, Sp, Hnd, Hea, Lsh, Psn, Str, Com, Crs, Fto, Agg, Inf, Ecc
        FROM attributes_active;
        """
        players = self.execute_query(query)
        return {player[0]: player[1:] for player in players}

    # Registra uma alteração na tabela de histórico
    def log_clubinfo_change(self, club_id, is_active, change_date):
        query = """
        INSERT INTO club_active_history (club_id, is_active, change_date)
        VALUES (%s, %s, %s);
        """
        self.execute_query(query, (club_id, is_active, change_date), fetch=False)

    # Função para selecionar clubes que tiveram mudança de status
    def get_club_active_history(self, date):
        query = f"SELECT club_id FROM club_active_history WHERE change_date = {date}"
        params = None

        return [row[0] for row in self.execute_query(query, params)]

    # Função para selecionar jogadores de um clube
    def get_player_active(self, club_id):
        query = f"SELECT id, club_id, name, position, nationality, age, rating FROM player_active WHERE club_id = {club_id}"
        params = None

        return [row[0] for row in self.execute_query(query, params)]

    # Função para mover jogadores de um clube ativo para inativo
    def move_player(self, player):
        converted_player = (
            int(player[0]),           # id
            int(player[1]),           # club_id
            str(player[2]),           # name
            str(player[3]),           # position
            str(player[4]),           # nationality
            int(player[5]),           # age
            float(player[6])          # rating
        )

        query = """
        INSERT INTO player_inactive (id, club_id, name, position, nationality, age, rating)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        self.execute_query(query, converted_player, fetch=False)

        query = f"""
        DELETE FROM player_active WHERE id = %s;
        """
        self.execute_query(query, (player[0],), fetch=False)
