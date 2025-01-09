from cassandra.cluster import Cluster

class ScyllaDBClient:
    def __init__(self):
        # Connect to ScyllaDB cluster
        self.cluster = Cluster(['localhost'])  # Replace with your ScyllaDB address
        self.session = self.cluster.connect()

        # Ensure keyspace and table exist
        self.create_keyspace_and_table()

    def create_keyspace_and_table(self):
        # Create keyspace if it doesn't exist
        self.session.execute("""
        CREATE KEYSPACE IF NOT EXISTS cdr_keyspace
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """)

        # Use the keyspace
        self.session.set_keyspace("cdr_keyspace")

        # Drop table if it exists
        self.session.execute("""
        DROP TABLE IF EXISTS daily_summary;
        """)

        # Create the table
        self.session.execute("""
        CREATE TABLE daily_summary (
            msisdn TEXT,
            usage_date DATE,
            usage_type TEXT,
            up_bytes BIGINT,
            down_bytes BIGINT,
            call_cost DOUBLE,
            data_cost DOUBLE,
            PRIMARY KEY ((msisdn, usage_date), usage_type)
        );
        """)


    def write_data(self, summarized_data):
        for record in summarized_data:
            print(f"Inserting record into ScyllaDB: {record}")  # Debugging line
            query = """
            INSERT INTO cdr_keyspace.daily_summary (
                msisdn, usage_date, usage_type, up_bytes, down_bytes, call_cost, data_cost
            ) VALUES (%s, %s, %s, %s, %s, %s, %s);
            """
            self.session.execute(query, (
                record['msisdn'],
                record['usage_date'],
                record['usage_type'],
                record['up_bytes'],
                record['down_bytes'],
                record['call_cost'],
                record['data_cost'],
            ))

            
    def query_data(self):
        # Query and print all data from daily_summary
        rows = self.session.execute("SELECT * FROM cdr_keyspace.daily_summary;")
        for row in rows:
            print(row)
 
if __name__ == "__main__":
    client = ScyllaDBClient()
    print("Data in daily_summary table:")
    client.query_data()
