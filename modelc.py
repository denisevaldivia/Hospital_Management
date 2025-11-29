# ------------------------------------
#   CASSANDRA MODELO
# ------------------------------------

def create_keyspace(session, keyspace, replication_factor):
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{
            'class': 'SimpleStrategy',
            'replication_factor': {replication_factor}
        }};
    """)
    print("[CASSANDRA] Keyspace OK")


def create_schema(session): pass


# ------------------------------------
#   QUERIES VACÍAS
# ------------------------------------

def query_1(session, user_input): pass
def query_2(session, user_input): pass
def query_3(session, user_input): pass
def query_4(session, user_input): pass
def query_5(session, user_input): pass
def query_7(session, user_input): pass
def query_8(session, user_input): pass
def query_9(session, user_input): pass
def query_10(session, user_input): pass
def query_11(session, user_input): pass
def query_12(session, user_input): pass
def query_13(session, user_input): pass
def query_14(session, user_input): pass
