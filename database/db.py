import os
from dotenv import load_dotenv
import mysql.connector

load_dotenv()

class Database:
    def __init__(self):
        self.host = os.getenv("MYSQL_HOST")
        self.user = os.getenv("MYSQL_USER")
        self.password = os.getenv("MYSQL_PASSWORD")
        self.database = os.getenv("MYSQL_DB")
        self.connection = None

    def connect(self):
        self.connection = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )
        return self.connection.cursor()

    def disconnect(self):
        if self.connection:
            self.connection.close()

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

    def log_attribute_change(self, player_id, column_name, old_value):
        query = """
        INSERT INTO attributes_history (player_id, column_name, old_value)
        VALUES (%s, %s, %s);
        """
        self.execute_query(query, (player_id, column_name, old_value), fetch=False)

    def update_players_batch(self, table, players_data):
        converted = [
            (int(p[0]), int(p[1]), str(p[2]), str(p[3]), str(p[4]), int(p[5]), float(p[6]))
            for p in players_data
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
        self.execute_query(query, converted, many=True, fetch=False)

    def update_attributes_batch(self, table, data):
        converted = [tuple(row) for row in data]
        query = f"""
        INSERT INTO attributes_{table} (
            id, Ref, Tck, Cre, Sht, Tmw, One, Mrk, Pas, Dri, Sp, Hnd, Hea, Lsh, Psn, Str, Com, Crs, Fto, Agg, Inf, Ecc
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            Ref = VALUES(Ref), Tck = VALUES(Tck), Cre = VALUES(Cre), Sht = VALUES(Sht),
            Tmw = VALUES(Tmw), One = VALUES(One), Mrk = VALUES(Mrk), Pas = VALUES(Pas),
            Dri = VALUES(Dri), Sp = VALUES(Sp), Hnd = VALUES(Hnd), Hea = VALUES(Hea),
            Lsh = VALUES(Lsh), Psn = VALUES(Psn), Str = VALUES(Str), Com = VALUES(Com),
            Crs = VALUES(Crs), Fto = VALUES(Fto), Agg = VALUES(Agg), Inf = VALUES(Inf), Ecc = VALUES(Ecc);
        """
        self.execute_query(query, converted, many=True, fetch=False)

    def get_clubinfo(self, clubs=None):
        if clubs:
            placeholders = ','.join(['%s'] * len(clubs))
            query = f"SELECT id FROM clubinfo WHERE id IN ({placeholders}) AND is_active = 1"
            return [row[0] for row in self.execute_query(query, clubs)]
        else:
            query = "SELECT id FROM clubinfo WHERE is_active = 1"
            return [row[0] for row in self.execute_query(query)]

    def get_clubinfo_with_is_inactive(self, start, end):
        query = "SELECT id, is_active FROM clubinfo WHERE is_active = 0 AND id >= %s AND id <= %s"
        result = self.execute_query(query, (start, end))
        return {row[0]: row[1] for row in result}

    def update_club_info(self, data):
        converted = [tuple(row) for row in data]
        query = """
        INSERT INTO clubinfo (
            id, team_name, short_name, manager_id, manager_name,
            stadium, country, league_id, league_name, rating,
            last_active, is_active
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
        self.execute_query(query, converted, many=True, fetch=False)

    def get_all_players_and_attributes(self):
        query = "SELECT id, Ref, Tck, Cre, Sht, Tmw, One, Mrk, Pas, Dri, Sp, Hnd, Hea, Lsh, Psn, Str, Com, Crs, Fto, Agg, Inf, Ecc FROM attributes_active"
        result = self.execute_query(query)
        return {row[0]: row[1:] for row in result}

    def log_clubinfo_change(self, club_id, is_active, change_date):
        query = """
        INSERT INTO club_active_history (club_id, is_active, change_date)
        VALUES (%s, %s, %s);
        """
        self.execute_query(query, (club_id, is_active, change_date), fetch=False)

    def get_club_active_history(self, date):
        query = "SELECT club_id FROM club_active_history WHERE change_date = %s"
        return [row[0] for row in self.execute_query(query, (date,))]

    def get_player_active(self, club_id):
        query = "SELECT id, club_id, name, position, nationality, age, rating FROM player_active WHERE club_id = %s"
        return self.execute_query(query, (club_id,))

    def move_player(self):
        # Step 1: Inserir jogadores inativos
        self.execute_query("""
            INSERT IGNORE INTO player_inactive (id, club_id, name, position, nationality, age, rating)
            SELECT p.id, p.club_id, p.name, p.position, p.nationality, p.age, p.rating
            FROM player_active p
            JOIN clubinfo c ON p.club_id = c.id
            WHERE c.is_active = 0
        """, fetch=False)

        # Step 2: Inserir atributos inativos
        self.execute_query("""
            INSERT IGNORE INTO attributes_inactive (id, Ref, Tck, Cre, Sht, Tmw, One, Mrk, Pas, Dri, Sp,
                                                    Hnd, Hea, Lsh, Psn, Str, Com, Crs, Fto, Agg, Inf, Ecc)
            SELECT a.id, a.Ref, a.Tck, a.Cre, a.Sht, a.Tmw, a.One, a.Mrk, a.Pas, a.Dri, a.Sp,
                a.Hnd, a.Hea, a.Lsh, a.Psn, a.Str, a.Com, a.Crs, a.Fto, a.Agg, a.Inf, a.Ecc
            FROM attributes_active a
            JOIN player_active p ON a.id = p.id
            JOIN clubinfo c ON p.club_id = c.id
            WHERE c.is_active = 0
        """, fetch=False)

        # Step 3: Deletar atributos ativos dos jogadores inativos
        self.execute_query("""
            DELETE a
            FROM attributes_active a
            JOIN player_active p ON a.id = p.id
            JOIN clubinfo c ON p.club_id = c.id
            WHERE c.is_active = 0
        """, fetch=False)

        # Step 4: Deletar jogadores ativos de clubes inativos
        self.execute_query("""
            DELETE p
            FROM player_active p
            JOIN clubinfo c ON p.club_id = c.id
            WHERE c.is_active = 0
        """, fetch=False)

        print("Transferência de jogadores inativos concluída.")

    def get_max_club_id(self):
        query = "SELECT MAX(id) FROM clubinfo"
        result = self.execute_query(query, fetch=True)
        return result[0][0] if result and result[0][0] else 0
