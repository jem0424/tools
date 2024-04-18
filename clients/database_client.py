from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from pymysql.err import OperationalError

from urllib.parse import quote

from utilities.utility_logger import utility_logger

logger = utility_logger()


class DatabaseClient(object):
    """
    Database Class to handle Database connections and execution
    """

    def __init__(self, database_credentials: dict, timeout=None):
        self.connection = database_credentials["connection"]
        self.user = database_credentials["user"]
        self.password = quote(database_credentials["pass"])
        self.host = database_credentials["host"]
        self.port = 3306
        self.schema = database_credentials["db"]
        self.SQLALCHEMY_DATABASE_URI = f"{self.connection}://{self.user}:{self.password}@{self.host}/{self.schema}"
        self.engine = None
        self.session = None
        self.timeout = timeout

    def __enter__(self):
        """
        built-in method will run as soon as class is instantiated. open the path of current working directory and create
        a local version in the temp folder of the project. The content is deserialized and dumped into that temp file
        """
        try:
            self.connect_db()
            my_session = sessionmaker(bind=self.engine)
            self.session = my_session()
            return self
        except Exception as e:
            logger.exception(f"DatabaseClient object creation failed: {e}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        built-in method that will execute at the end of a with statement. The remove command will look in the path
        parameter and delete the corresponding file mentioned
        :param exc_type:
-       :param exc_val:
-       :param exc_tb:
        """
        try:
            self.session.close()
            if self.engine:
                self.engine.dispose()
            else:
                raise Exception("Engine is None")
        except Exception as e:
            logger.exception(f"DatabaseClient object destruction failed: {e}")

    def create(self):
        """
        Create method will run connect_db command as soon as it is called
        :return self object
        """
        try:
            self.connect_db()
            return self
        except OperationalError as e:
            logger.exception(f"{self.__class__.create.__qualname__}: {e}")
            raise e

    def close(self):
        """
        Closes session and engine when done executing queries
        """
        try:
            self.session.close()
            self.engine.dispose()
        except Exception as e:
            logger.exception(f"{self.__class__.close.__qualname__}: {e}")
            raise e

    def connect_db(self):
        """
        Function to connect to database using the credentials passed through as a connection string via
        create_engine.
        """
        try:
            if self.timeout:
                # Used for testing
                self.engine = create_engine(self.SQLALCHEMY_DATABASE_URI, connect_args={"connect_timeout": self.timeout})
            else:
                self.engine = create_engine(self.SQLALCHEMY_DATABASE_URI)
            self.session = Session(self.engine)
            self.engine.connect()
        except OperationalError as e:
            if "timeout" in str(e).lower():
                print("Database connection timed out")
            else:
                print("Database connection failed:", str(e))
        except Exception as e:
            logger.exception(f"Error connecting to database: {e}")

    def execute_query(self, query: str) -> list:
        """
        Executes query passed as a string and returns results
        :param query: SQL statement preferably one that returns a row (SELECT commands)
        :return: list of rows
        """
        try:
            result = self.session.execute(text(query))
            records = [row for row in result]
            return records
        except OperationalError as e:
            logger.error(f"Connection to {self.host} failed: {e}")
            raise e
        except Exception as e:
            logger.exception(f"Error executing query: {e}")

