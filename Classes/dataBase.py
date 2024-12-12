import mysql, json, os, logging, warnings
from mysql.connector import Error

with open(f'Settings/settings.json') as file:
    settings = json.load(file)

class DataBase:
    def __init__(self, db_title, autocreate = False):
        self.db_title = db_title
        self.connector = self.create_server_connection(settings['host'], settings['user'], settings['password'])
        self.cursor = self.connector.cursor()
        self.fields = settings['tables']
        self.tables = {
            'tickers': self.ticker_table_query,
            'data': self.data_table_query
        }

        self.connect_db(autocreate)

    @staticmethod
    def create_server_connection(hostName, userName, userPassword):
        connection = None
        try:
            connection = mysql.connector.connect(
                host=hostName,
                user=userName,
                passwd=userPassword
            )
            print("MySQL Database connection successful")
        except Error as err:
            print(f"Error: '{err}'")

        return connection
    
    def available_db(self):
        self.cursor.execute("SHOW DATABASES")
        db_files = self.cursor.fetchall()
        return [item[0] for item in db_files]

    @staticmethod
    def is_id(items):
        if not isinstance(items, list):
            items = [items]
        ids = []
        titles = []
        for item in items:
            if item.replace('.', '').replace(' ', '').isalpha():
                titles.append(item)
            else:
                ids.append(item)
        return {'ids': ids, 'titles': titles}

    # @staticmethod
    # def url_request(url, return_type = 'df'):
    #     for i in range(3, 0, -1):
    #         try:
    #             response = requests.get(url, timeout=10)
    #             if response.status_code != 200:
    #                 return False
    #             if return_type=='df':
    #                 return pd.DataFrame(json.load(io.StringIO(response.content.decode('utf-8'))))
    #             elif return_type=='json':
    #                 return json.load(io.StringIO(response.content.decode('utf-8')))
    #         except TimeoutError:
    #             if i < 3:
    #                 logging.warning(f'TimeOut error was observed, more attempts {i}')
    #             else:
    #                 logging.warning(f'TimeOut observed')
    #                 warnings.warn('URL request get gets TimeOut three times')
    #                 return False
    #         except:
    #             logging.warning(f'data is not available')
    #             return False

    def connect_db(self, autocreate=False):
        available = self.available_db()
        
        if self.db_title not in available:
            if autocreate:
                logging.info(f"DataBase {self.db_title} not exist")
                self.cursor.execute(f"CREATE DATABASE {self.db_title}")
                self.create_tables(self.tables)
            else:
                logging.info(f'DataBase for {self.db_title} not exist')
                raise Exception(f'DataBase for {self.db_title} not exist. The list of available db is {available}')

        self.connector = mysql.connector.connect(
                                                host = settings['host'], 
                                                user = settings['user'], 
                                                password = settings['password'],
                                                database = self.db_title
                                                )
        self.cursor = self.connector.cursor()
        self.check_tables()
        logging.info(f'DataBese {self.db_title} successfully connected')

    def create_tables(self, tables=[]):
        for table in tables:
            logging.info(f'Creating {table} table for {self.db_title} DataBase')
            self.run_query(self.tables[table]())

    def ticker_table_query(self):
        feild = 'tickers'
        querry = ' '.join([' '.join(item) for item in self.fields[feild]])
        return [f"CREATE TABLE IF NOT EXISTS {feild} (\
                            {querry}\
                            CONSTRAINT PK_ticker PRIMARY KEY (TICKER, EXCHANGE))"]

    def data_table_query(self):
        feild = 'data'
        querry = ' '.join([' '.join(item) for item in self.fields[feild]])
        return [f"CREATE TABLE IF NOT EXISTS {feild} (\
                            {querry}\
                            CONSTRAINT PK_ticker PRIMARY KEY (ID, DATE))",
                "CREATE INDEX idx_data_id ON data(ID)",
                "CREATE INDEX idx_data_date ON data(date)"
                ]

    def run_query(self, query=[]):
        for item in query:
            cursor = self.connector.cursor()
            cursor.execute(item)
        return True

    def check_tables(self):
        tebles_not_exist = []
        cursor = self.connector.cursor()
        cursor.execute(f"SHOW TABLES")
        tables_exist = [item[0] for item in cursor.fetchall()]
        for table_name in self.tables.keys():
            if table_name not in tables_exist:
                tebles_not_exist.append(table_name)
            # else:
                # self.check_fields(table_name)
        if len(tebles_not_exist) != 0:
            self.create_tables(tebles_not_exist)
        logging.info('All tables are existed')
        return True

    # def check_fields(self, table):
    #     for item in self.fields[table]:
    #         structure = self.db.execute(f"SELECT sql FROM sqlite_schema WHERE name = '{table}';").fetchall()
    #         if item[0] not in structure[0][0]:
    #             self.db.execute(f"ALTER TABLE {table} ADD {item[0]} {item[1]} {item[2]} DEFAULT {self.defaults[item[1]]};")
    #             self.db.commit()
    #     return True


    def close(self):
        self.db.close()
        logging.info(f'DataBase {self.db_title} was closed')
        return True