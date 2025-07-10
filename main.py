import pyodbc
import os
import csv
from datetime import datetime
from typing import List, Optional, Tuple, Any, Iterator


class WinCC_AlarmLogging_Exporter:
    """
    A class to export data (AlarmLogging) from specific WinCC views in SQL Server databases.
    Exports target views from all databases containing '_ALG_' in their names.
    """

    def __init__(self, server: str = "") -> None:
        """
        Initialize the exporter with server connection information.

        Args:
            server: SQL Server instance name (default: local WINCC instance)
        """
        self.server: str = server or os.environ['COMPUTERNAME'] + "\\WINCC"
        self.target_views: List[str] = [
            "AlgViewENU_ID_OPT",  # English view
            "AlgViewRUS_ID_OPT",  # Russian view
            "AlgViewDEU_ID_OPT",  # German view
                                  # other languages can be added
        ]
        self.conn: Optional[pyodbc.Connection] = None  # Database connection
        self.cur: Optional[pyodbc.Cursor] = None  # Database cursor

    def export_alarmlogging_data(self, output_dir: str = "wincc_alarmlogging_export") -> None:
        """
        Export data from target views in all _ALG_ databases.

        Args:
            output_dir: Directory to save exported CSV files
        """
        try:
            self._connect_to_master()
            alg_databases: List[str] = self._get_alg_databases()

            if not alg_databases:
                print("No databases with '_ALG_' in name found!")
                return

            os.makedirs(output_dir, exist_ok=True)

            for db_name in alg_databases:
                self._process_database_views(db_name, output_dir)

        finally:
            self.close()

    def _connect_to_master(self) -> None:
        """Establish connection to master database to find other databases. Set cursor"""
        self.conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};"
            f"SERVER={self.server};"
            f"DATABASE=master;"
            f"Trusted_Connection=yes;"
            f"Encrypt=no;TrustServerCertificate=yes;",
            autocommit=True
        )
        self.cur = self.conn.cursor()

    def _get_alg_databases(self) -> List[str]:
        """Retrieve list of all _ALG_ databases that are online."""
        self.cur.execute("""
            SELECT name 
            FROM sys.databases 
            WHERE name LIKE '%\\_ALG\\_%' ESCAPE '\\'
            AND state = 0  -- Only online databases
            ORDER BY name
        """)
        return [row[0] for row in self.cur.fetchall()]

    def _process_database_views(self, db_name: str, output_dir: str) -> None:
        """
        Process and export views from a single database.

        Args:
            db_name: Database name to process
            output_dir: Base output directory for exports
        """
        try:
            print(f"\n🔍 Checking database: {db_name}")
            conn: pyodbc.Connection = pyodbc.connect(
                f"DRIVER={{SQL Server}};"
                f"SERVER={self.server};"
                f"DATABASE={db_name};"
                f"Trusted_Connection=yes;",
                autocommit=True
            )
            cur: pyodbc.Cursor = conn.cursor()

            # Check for target views
            for view_name in self.target_views:
                if self._check_view_exists(cur, view_name):
                    self._export_view(cur, db_name, view_name, output_dir)
                else:
                    print(f"View {view_name} not found in {db_name}")

        except Exception as e:
            print(f"Error processing {db_name}: {e}")
        finally:
            conn.close()

    def _check_view_exists(self, cursor: pyodbc.Cursor, view_name: str) -> bool:
        """
        Check if a view exists in the database.

        Args:
            cursor: Database cursor
            view_name: Name of view to check

        Returns:
            True if view exists, False otherwise
        """
        cursor.execute("""
            SELECT 1 
            FROM INFORMATION_SCHEMA.VIEWS 
            WHERE TABLE_NAME = ?
        """, view_name)
        return cursor.fetchone() is not None

    def _export_view(self,
                     cursor: pyodbc.Cursor,
                     db_name: str,
                     view_name: str,
                     output_dir: str) -> None:
        """
        Export data from a view to CSV file.

        Args:
            cursor: Database cursor
            db_name: Source database name
            view_name: View to export
            output_dir: Base output directory
        """
        try:
            # Create database-specific directory
            db_dir: str = os.path.join(output_dir, db_name)
            os.makedirs(db_dir, exist_ok=True)

            csv_path: str = os.path.join(db_dir, f"{view_name}.csv")

            # Get data from view
            cursor.execute(f"SELECT * FROM [{view_name}]")
            columns: List[str] = [column[0] for column in cursor.description]

            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer: csv.writer = csv.writer(f, delimiter=';')
                writer.writerow(columns)

                # Export data in batches
                while True:
                    batch: List[Tuple[Any, ...]] = cursor.fetchmany(10_000)
                    if not batch:
                        break
                    writer.writerows(batch)

            print(f"✅ Success: {db_name}/{view_name}")

        except Exception as e:
            print(f"❌ Error exporting {view_name}: {e}")

    def close(self) -> None:
        """Close database connection if it exists."""
        if self.conn:
            self.conn.close()


if __name__ == "__main__":

    exporter = WinCC_AlarmLogging_Exporter("172.16.0.244\\WINCC")
    exporter.export_alarmlogging_data()