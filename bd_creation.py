import pandas as pd
import ast
import sqlite3
import os

# --- 1. Configuração e Leitura de Dados ---
tsv_file = 'ts_discography.tsv'
db_filename = 'taylor_swift.db'

print(f"A ler o ficheiro {tsv_file}...")
df = pd.read_csv(tsv_file, sep='\t')

# Função para limpar as listas que vêm como texto (ex: "['Taylor Swift', 'Liz Rose']")
def clean_list(s):
    try:
        # Se for string, tenta converter. Se for float (NaN), devolve lista vazia
        if pd.isna(s): return []
        return ast.literal_eval(s)
    except:
        return []

# --- 2. Preparar a Base de Dados ---
if os.path.exists(db_filename):
    os.remove(db_filename)
    print(f"Base de dados anterior removida.")

conn = sqlite3.connect(db_filename)
cursor = conn.cursor()
cursor.execute("PRAGMA foreign_keys = ON;")

print("A criar tabelas...")

# --- 3. Criação das Tabelas (Schema 'Excellence') ---

# ERAS (Baseada na coluna 'category')
cursor.execute("""
CREATE TABLE Eras (
    era_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE
);
""")

# ALBUMS
cursor.execute("""
CREATE TABLE Albums (
    album_id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT,
    url TEXT,
    era_id INTEGER,
    FOREIGN KEY (era_id) REFERENCES Eras(era_id)
);
""")

# PEOPLE (Lista única de todas as pessoas)
cursor.execute("""
CREATE TABLE People (
    person_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE
);
""")

# ROLES (Funções fixas)
cursor.execute("""
CREATE TABLE Roles (
    role_id INTEGER PRIMARY KEY AUTOINCREMENT,
    role_name TEXT UNIQUE
);
""")

# TAGS
cursor.execute("""
CREATE TABLE Tags (
    tag_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE
);
""")

# SONGS
cursor.execute("""
CREATE TABLE Songs (
    song_id INTEGER PRIMARY KEY AUTOINCREMENT,
    album_id INTEGER,
    title TEXT,
    track_number INTEGER,
    url TEXT,
    release_date TEXT,
    page_views INTEGER,
    lyrics TEXT,
    FOREIGN KEY (album_id) REFERENCES Albums(album_id)
);
""")

# SONG_PEOPLE (Tabela de Junção M:N - Quem fez o quê)
cursor.execute("""
CREATE TABLE Song_People (
    song_id INTEGER,
    person_id INTEGER,
    role_id INTEGER,
    PRIMARY KEY (song_id, person_id, role_id),
    FOREIGN KEY (song_id) REFERENCES Songs(song_id),
    FOREIGN KEY (person_id) REFERENCES People(person_id),
    FOREIGN KEY (role_id) REFERENCES Roles(role_id)
);
""")

# SONG_TAGS (Tabela de Junção M:N)
cursor.execute("""
CREATE TABLE Song_Tags (
    song_id INTEGER,
    tag_id INTEGER,
    PRIMARY KEY (song_id, tag_id),
    FOREIGN KEY (song_id) REFERENCES Songs(song_id),
    FOREIGN KEY (tag_id) REFERENCES Tags(tag_id)
);
""")

# --- 4. Inserção de Dados de Referência ---
print("A inserir dados de referência (Eras, Pessoas, Roles, Tags)...")

# Inserir Eras únicas
eras = df['category'].dropna().unique()
for era in eras:
    cursor.execute("INSERT OR IGNORE INTO Eras (name) VALUES (?)", (era,))

# Inserir Roles fixas
roles = ['Artist', 'Writer', 'Producer']
for role in roles:
    cursor.execute("INSERT OR IGNORE INTO Roles (role_name) VALUES (?)", (role,))

# Extrair e Inserir Pessoas e Tags únicas
all_people = set()
all_tags = set()

for _, row in df.iterrows():
    # Pessoas das 3 colunas
    for col in ['song_artists', 'song_writers', 'song_producers']:
        for person in clean_list(row[col]):
            all_people.add(person.strip())
    # Tags
    for tag in clean_list(row['song_tags']):
        all_tags.add(tag.strip())

for p in all_people:
    cursor.execute("INSERT OR IGNORE INTO People (name) VALUES (?)", (p,))

for t in all_tags:
    cursor.execute("INSERT OR IGNORE INTO Tags (name) VALUES (?)", (t,))

conn.commit()

# --- 5. Criar Mapas de ID (Para inserção rápida) ---
# Em vez de fazer SELECT a cada linha, carregamos os IDs para dicionários Python
era_map = pd.read_sql("SELECT name, era_id FROM Eras", conn).set_index('name')['era_id'].to_dict()
person_map = pd.read_sql("SELECT name, person_id FROM People", conn).set_index('name')['person_id'].to_dict()
role_map = pd.read_sql("SELECT role_name, role_id FROM Roles", conn).set_index('role_name')['role_id'].to_dict()
tag_map = pd.read_sql("SELECT name, tag_id FROM Tags", conn).set_index('name')['tag_id'].to_dict()

# --- 6. Inserção Principal (Álbuns, Músicas e Relações) ---
print("A inserir Músicas e relacionamentos...")

# Cache de Álbuns inseridos para evitar duplicados e queries extra
albums_inserted = {} # {'Titulo do Album': album_id}

for index, row in df.iterrows():
    # 6.1 Inserir Álbum (se ainda não tiver sido inserido neste loop)
    album_title = row['album_title']
    if album_title not in albums_inserted:
        era_id = era_map.get(row['category'])
        cursor.execute("INSERT INTO Albums (title, url, era_id) VALUES (?, ?, ?)", 
                       (album_title, row['album_url'], era_id))
        albums_inserted[album_title] = cursor.lastrowid
    
    album_id = albums_inserted[album_title]

    # 6.2 Inserir Música
    cursor.execute("""
        INSERT INTO Songs (album_id, title, track_number, url, release_date, page_views, lyrics)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (album_id, row['song_title'], row['album_track_number'], row['song_url'], 
          row['song_release_date'], row['song_page_views'], row['song_lyrics']))
    
    song_id = cursor.lastrowid

    # 6.3 Inserir Relações: Song_People
    # Função auxiliar para processar cada coluna de pessoas
    def insert_relations(col_name, role_name):
        people_list = clean_list(row[col_name])
        role_id = role_map.get(role_name)
        for person_name in people_list:
            person_id = person_map.get(person_name.strip())
            if person_id and role_id:
                cursor.execute("""
                    INSERT OR IGNORE INTO Song_People (song_id, person_id, role_id)
                    VALUES (?, ?, ?)
                """, (song_id, person_id, role_id))

    insert_relations('song_artists', 'Artist')
    insert_relations('song_writers', 'Writer')
    insert_relations('song_producers', 'Producer')

    # 6.4 Inserir Relações: Song_Tags
    tags_list = clean_list(row['song_tags'])
    for tag_name in tags_list:
        tag_id = tag_map.get(tag_name.strip())
        if tag_id:
            cursor.execute("""
                INSERT OR IGNORE INTO Song_Tags (song_id, tag_id)
                VALUES (?, ?)
            """, (song_id, tag_id))

conn.commit()
conn.close()

print(f"\nSucesso! A base de dados '{db_filename}' foi criada e povoada.")