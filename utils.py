# pour manipuler des dates
import datetime as dt

# Pour faire des recherches de fichiers dans des répertoires (type 'ls')
import glob

from io import StringIO

import pandas as pd

# Pour accéder à la fonction remove, qui supprime des fichiers
import os

import requests

# Pour lire et écrire dans des BDD
from sqlalchemy import create_engine, func, MetaData, select, Table

import time

# Pour télécharger des fichiers
import urllib

# Pour zipper/dézipper
import zipfile

def create_url(date):
    """Crée le nom d'url à aller télécharger"""
    url = 'http://files.data.gouv.fr/sirene/sirene_'
    
    url += str(date.year)
    
    # Calculer le nombre de jours depuis le 1er janvier
    url += str((date - dt.date(date.year, 1, 1)).days + 1).zfill(3)
    
    url += '_E_Q.zip'
    
    return url

def download(date):
    url = create_url(date)
    zipfilename = 'tmp.zip'
    
    # download
    try:
        urllib.request.urlretrieve(url, zipfilename)
    except urllib.request.HTTPError:
        msg = ("Pas de fichier trouvé pour ce jour.")
        raise Exception(msg)
    
    return zipfilename

def unzip(zipfilename):
    zip_ref = zipfile.ZipFile(zipfilename, 'r')
    zip_ref.extractall('.')
    zip_ref.close()

    os.remove(zipfilename)
    
    # Get CSV name
    csvfilename = glob.glob('sirc-*.csv')[0]
    
    return csvfilename

def fetch_df(date):
    zipfilename = download(date)
    
    csvfilename = unzip(zipfilename)
    
    df = pd.read_csv(csvfilename, sep=';', encoding='iso-8859-1',
                     low_memory=False, dtype=str)

    os.remove(csvfilename)
    
    return df

def check_date(date):
    """Renvoie True si le fichier à cette date est déjà en base"""
    # Crée la connexion
    engine = create_engine('sqlite:///data.sqlite')
    connection = engine.connect()
    metadata = MetaData()
    
    # Créer l'objet Table
    try:
        rests = Table('restaurants', metadata, autoload=True,
                      autoload_with=engine)
        
        # Renvoie le nombre de lignes correspondants à cette date
        stmt = select([func.count(rests)])
        stmt = stmt.where(rests.c.CREATEDDATE == date)
        
        num = connection.execute(stmt).scalar()

    except Exception as e:
        # cas où la table restaurants n'existe pas encore
        num = 0

    # Ferme la connexion
    connection.close()
    
    return (num > 0)

def geoloc(df, columns, postcode, verbose=False):
    """Géo-localise les adresses présentes dans les colonnes d'un df"""
    # Ecrire les données dans un csv
    file = StringIO()
    df[columns].to_csv(file, index=False)
    
    # Revenir au début du fichier
    file.seek(0)
    
    if verbose:
        t = time.time()
    
    # Envoyer les adresses à l'API adresse.data.gouv.fr
    r = requests.post("https://api-adresse.data.gouv.fr/search/csv/",
                      timeout=600,
                      files={'data': file},
                      params={'postcode': postcode})
    
    # Test d'erreur serveur
    if r.text.startswith('<html>'):
        raise Exception("Geoloc failed.")
    
    df_res = pd.read_csv(StringIO(r.text))
    
    if verbose:
        print("Requête effectuée en {temps}".format(
            temps=time.time() - t))
        print("{x}% d'adresses géo-localisées.".format(
            x=100 * (1 - df_res['latitude'].isnull().mean())))
    
    # Lire les données dans un DataFrame
    return df_res

def update(date):
    """Mise à jour quotidienne à partir du fichier SIRENE"""
    # test si la date est déjà en base
    if check_date(date):
        return

    # téléchargement et extraction du fichier
    df = fetch_df(date)

    # Filtrer uniquement sur les établissements dont l'activité contient le
    # terme restauration.
    c = df['LIBAPET'].str.contains('[rR]estauration')
    df = df.loc[c]

    # Extraire uniquement les créations ou suppressions d'établissements.
    c = (df['VMAJ'] == 'C') | (df['VMAJ'] == 'E')
    df = df.loc[c]

    """
    Création de la table restaurants
    """
    dft = pd.DataFrame()

    # SIREN
    dft['SIREN'] = df['SIREN']

    # SIRET
    dft['SIRET'] = df['SIREN'] + df['NIC']

    # Nom de l'entreprise
    dft['NOMEN'] = df['NOMEN_LONG']

    # ADRESSE
    dft['ADRESSE'] = (df['NUMVOIE'].fillna(' ') + ' ' + 
                      df['INDREP'].fillna(' ') + ' ' +
                      df['TYPVOIE'].fillna(' ') + ' ' +
                      df['LIBVOIE'].fillna(' '))

    # Remplace plusieurs espaces par un seul
    dft['ADRESSE'] = dft['ADRESSE'].replace(' +', ' ')

    # COM
    dft['COM'] = df['DEPET'] + df['COMET']

    # CODPOS
    dft['CODPOS'] = df['CODPOS']

    # LIBCOM
    dft['LIBCOM'] = df['LIBCOM']

    # ACT
    dft['ACT'] = df['APET700']

    # DEP
    dft['DEP'] = df['DEPET']

    # DATE
    def create_date(chaine):
        return dt.date(int(chaine[:4]), int(chaine[4:6]), int(chaine[-2:]))

    dft['DATE'] = df['DATEVE'].map(create_date)

    # STATUT
    dft['STATUT'] = df['VMAJ'].str.replace('E', 'S')

    # CREATEDDATE
    dft['CREATEDDATE'] = date

    # lat/lon
    df_geo = geoloc(dft, columns=['ADRESSE', 'COM', 'CODPOS', 'LIBCOM'],
                    postcode='CODPOS')
    dft = dft.reset_index(drop=True)
    dft['latitude'] = df_geo['latitude']
    dft['longitude'] = df_geo['longitude']

    """
    Export de la table restaurants
    """
    # Ouverture de la connexion vers la BDD
    engine = create_engine('sqlite:///data.sqlite')
    connection = engine.connect()

    # Ecrire
    dft.to_sql('restaurants', connection, if_exists='append', index=False)

    # Refermer la connection
    connection.close()

    """
    Création et export de la table activites
    """
    # Ouverture de la connexion vers la BDD
    connection = engine.connect()

    # Ecriture
    df[['APET700', 'LIBAPET']].drop_duplicates() \
                              .rename(columns={
                                  'APET700': 'ACT',
                                  'LIBAPET': 'LIBACT'
                              }) \
                              .to_sql('activites', connection,
                                      if_exists='replace', index=False)

    # Refermer la connection
    connection.close()

def clean_table(tablename):
    """Supprime les lignes en double"""
    # Créer la connexion
    engine = create_engine('sqlite:///data.sqlite')
    connection = engine.connect()
    
    # Lire les données
    df = pd.read_sql_table(tablename, connection)
    
    # Supprime les doublons
    df = df.drop_duplicates()
    
    # Ecraser la table
    df.to_sql(tablename, connection, if_exists='replace', index=False)
    
    # Fermer la connexion
    connection.close()
    
    return