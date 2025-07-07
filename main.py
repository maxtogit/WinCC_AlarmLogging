import pyodbc
import os
import csv
from datetime import datetime


class WinCCViewsExporter:
    def __init__(self, server=""):
        self.server = server or os.environ['COMPUTERNAME'] + "\\WINCC"
        self.target_views = [
            "AlgViewENU_ID_OPT",  # Английское представление
            "AlgViewRUS_ID_OPT",  # Русское представление
            "AlgViewDEU_ID_OPT",  # DEU

        ]
        self.conn = None
        self.cur = None

    def export_views_data(self, output_dir="wincc_views_export"):
        """Экспортировать данные из целевых представлений во всех _ALG_ базах"""
        try:
            self._connect_to_master()
            alg_databases = self._get_alg_databases()

            if not alg_databases:
                print("Не найдено баз данных с _ALG_ в имени!")
                return

            os.makedirs(output_dir, exist_ok=True)

            for db_name in alg_databases:
                self._process_database_views(db_name, output_dir)

        finally:
            self.close()

    def _connect_to_master(self):
        """Подключение к master для поиска баз"""
        self.conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};"
            f"SERVER={self.server};"
            f"DATABASE=master;"
            #"UID=remote;PWD=20_Com_05;",
            f"Trusted_Connection=yes;"
            f"Encrypt=no;TrustServerCertificate=yes;",
            autocommit=True
        )
        self.cur = self.conn.cursor()

    def _get_alg_databases(self):
        """Получить список всех _ALG_ баз"""
        self.cur.execute("""
            SELECT name 
            FROM sys.databases 
            WHERE name LIKE '%\\_ALG\\_%' ESCAPE '\\'
            AND state = 0  -- Только онлайн-базы
            ORDER BY name
        """)
        return [row[0] for row in self.cur.fetchall()]

    def _process_database_views(self, db_name, output_dir):
        """Обработать представления в одной базе"""
        try:
            print(f"\n🔍 Проверка базы: {db_name}")
            conn = pyodbc.connect(
                f"DRIVER={{SQL Server}};"
                f"SERVER={self.server};"
                f"DATABASE={db_name};"
                f"Trusted_Connection=yes;",
                autocommit=True
            )
            cur = conn.cursor()

            # Проверяем наличие целевых представлений
            for view_name in self.target_views:
                if self._check_view_exists(cur, view_name):
                    self._export_view(cur, db_name, view_name, output_dir)
                else:
                    print(f"Представление {view_name} не найдено в {db_name}")

        except Exception as e:
            print(f"Ошибка при обработке {db_name}: {e}")
        finally:
            conn.close()

    def _check_view_exists(self, cursor, view_name):
        """Проверить существование представления"""
        cursor.execute("""
            SELECT 1 
            FROM INFORMATION_SCHEMA.VIEWS 
            WHERE TABLE_NAME = ?
        """, view_name)
        return cursor.fetchone() is not None

    def _export_view(self, cursor, db_name, view_name, output_dir):
        """Экспорт данных из представления в CSV"""
        try:
            # Создаем папку для базы
            db_dir = os.path.join(output_dir, db_name)
            os.makedirs(db_dir, exist_ok=True)

            csv_path = os.path.join(db_dir, f"{view_name}.csv")

            # Получаем данные из представления
            cursor.execute(f"SELECT * FROM [{view_name}]")
            columns = [column[0] for column in cursor.description]

            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter=';')
                writer.writerow(columns)

                # Выгружаем данные порциями
                while True:
                    batch = cursor.fetchmany(10_000)
                    if not batch:
                        break
                    writer.writerows(batch)

            print(f"✅ Успешно: {db_name}/{view_name}")

        except Exception as e:
            print(f"❌ Ошибка при экспорте {view_name}: {e}")

    def close(self):
        if self.conn:
            self.conn.close()


if __name__ == "__main__":
    exporter = WinCCViewsExporter("172.16.0.244\\WINCC")
    exporter.export_views_data()