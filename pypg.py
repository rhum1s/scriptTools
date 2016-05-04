# -*- coding:utf-8 -*-
"""
PyPg - R. Souweine, 2016.
"""

import sys
import os.path
import ConfigParser
from datetime import datetime 
import psycopg2
import psycopg2.extras 
import pandas.io.sql as psql
import geopandas as gpd
from sqlalchemy import create_engine
import warnings
warnings.simplefilter(action = "ignore", category = FutureWarning)

class PgDb:
    """
    Crée un objet base de données, connecté à PostgreSQL.
    Nécessite un fichier de configuration *.cfg.
    Ouvre automatiquement une connexion à la création de l'objet.
    """
    def __init__(self, config_file, debug=False, con=None, cur=None):
        """
        Initialisation et connection
        """        
        # Lecture de la configuration
        if os.path.isfile(config_file):
            self.config_file = config_file
         
            self.config = ConfigParser.ConfigParser()
            self.config.read(config_file)
            
            self.host   = self.ConfigSectionMap(self.config, "Connection")['host']  
            self.lgn    = self.ConfigSectionMap(self.config, "Connection")['lgn'] 
            self.pwd    = self.ConfigSectionMap(self.config, "Connection")['pwd'] 
            self.bdd    = self.ConfigSectionMap(self.config, "Connection")['bdd']
            self.port   = self.ConfigSectionMap(self.config, "Connection")['port'] 
            self.debug  = debug
        else:
            self.error("%s is doesn't exists." % config_file, exit=True)
        
        # Connection à  pgsql
        try:
            self.con = psycopg2.connect(host=self.host,user=self.lgn, 
                                        password=self.pwd, database=self.bdd) 
            self.cur = self.con.cursor(cursor_factory = 
                                        psycopg2.extras.RealDictCursor)
        except psycopg2.DatabaseError, e:
            self.error("%s" % e)  
        
        # Creation du moteur SQL Alchemy 
        self.engine = create_engine("postgresql://%s:%s@%s:%s/%s" % (self.lgn, self.pwd, self.host, self.port, self.bdd))
        
        if self.debug == True:
            self.info("Connecté à %s" % self.bdd)

    def connect_to_db(self, db_name):
        """
        Disconnect then connect to new database.
        """
        self.disconnect()
        self.bdd = db_name
        try:
            self.con = psycopg2.connect(host=self.host,user=self.lgn, 
                                            password=self.pwd, database=self.bdd)  
            self.cur = self.con.cursor(cursor_factory = 
                                        psycopg2.extras.RealDictCursor)                                        
        except psycopg2.DatabaseError, e:
            self.error("%s" % e) 
        
    def info(self, msg):
        print "%s PgDb > %s" % (datetime.now().strftime('%Y-%m-%d %H:%M'), msg)
        
    def warning(self, msg):
        print "%s PgDb > WARNING: %s" % (datetime.now().strftime('%Y-%m-%d %H:%M'), msg)
        
    def error(self, msg, exit=False):        
        print "%s PgDb > ERROR: %s" % (datetime.now().strftime('%Y-%m-%d %H:%M'), msg)
        if exit is True:
            sys.exit()
        
    def ConfigSectionMap(self, config_object, section):
        """
        Mapping des sections d'un fichier de config.
        """
        section_dict = {}
        options = config_object.options(section)
        
        for option in options:
            try:
                section_dict[option] = config_object.get(section, option)
                if section_dict[option] == -1:
                    self.warning("skip: %s" % option)
            except:
                self.warning("exception on %s!" % option)
                section_dict[option] = None
        
        return section_dict 

    def rm_accents(self, txt):
        """
        Supprime les caracters speciaux
        """
        for ch in ["é","è", "ê"]:
            txt = txt.replace(ch, "e")
        for ch in ["â","à"]:
            txt = txt.replace(ch, "a")
        for ch in ["ô"]:
            txt = txt.replace(ch, "o")  
        for ch in ["û", "ù"]:
            txt = txt.replace(ch, "u") 
        for ch in ["î", "i"]:
            txt = txt.replace(ch, "i")            

        return txt        
    
    def format_null_values(self, sql):
        """
        Replace Python None values to PostgreSQL NULL.
        """
        sql = sql.replace("None", "NULL")
        sql = sql.replace("'NULL'", "NULL")
        
        return sql
    
    def disconnect(self):
        """
        Deconnexion de la session pgsql
        """
        try:
            self.cur.close()
            self.con.close()
            if self.debug is True:
                self.info("Déconnecté de %s" % self.bdd)
        except psycopg2.DatabaseError, e:
            self.error("%s" % e)    

    def select(self, sql, commit=True, df=True):
        """
        Execute une requete select pgsql et retourne 
        le résultat si il y en a un et la sortie
        console.
        """        
        try:
            if self.debug == True:
               self.info("Sélection - %s" %sql)
                
            if df == True:
                df = psql.frame_query(sql, con=self.con)
                
                if len(df) == 0:
                    self.warning("... Table vide")
                
                if self.debug == True:
                    self.info("... Requête executée")
                return df
            else:
                self.cur.execute(sql)
                if self.debug == True:
                    self.info("... Requête executée: %s " %self.cur.statusmessage)
                return self.cur.fetchall()  
                 
        except psycopg2.DatabaseError, e:
            self.error("%s" %e)

    def execute(self, sql, commit=True, accents=False):
        """
        Execute une requete pgsql qui ne renvoie pas de résultats.
        """          
        try:
            if accents is True:
                self.info("... Suppression des accents")
                sql = self.rm_accents(sql)            
            
            sql = self.format_null_values(sql)
            
            if self.debug == True:
                self.info("Execution - %s" %sql)
            
            psql.execute(sql, con=self.con, cur=self.cur)
            self.con.commit()

            if self.debug == True:
                self.info("... Requete %s exécutée." %(self.cur.statusmessage))
                 
        except psycopg2.DatabaseError, e:
            self.error("%s" %e)
 
    def insert_df(self, df, schema, table, if_exists="append", index=False): 
        """
        Insertion d'une dataframe pandas dans PostgreSQL
        if_exists : {‘fail’, ‘replace’, ‘append’}
        """
        if self.debug is True:
            self.info("Insertion de DataFrame dans %s%s mode = %s." %(schema, table, if_exists))
        df.to_sql(table, self.engine, schema=schema, if_exists=if_exists, index=index)
        if self.debug is True:
            self.info("... Insertion de DataFrame exécutée.")
        
    def maintenance(self, sql):
        """
        Permets d'effectuer des requêtes de maintenance 
        sans se soucier des transactions. 
        (Pas de transactions pour ce type de requête.)
        """
        try:
            if self.debug == True:
                self.info("Maintenance - %s" %sql)
            
            self.old_isolation_level = self.con.isolation_level  
            self.con.set_isolation_level(0)
            psql.execute(sql, con=self.con, cur=self.cur)
            self.con.set_isolation_level(self.old_isolation_level)
            
            if self.debug == True:
                self.info("... Requete %s exécutée." %(self.cur.statusmessage))
            
        except psycopg2.DatabaseError, e:
            self.error("%s" %e)

    def geoselect(self, sql, geom_col="geom"):
        """
        Select PostgreSQL / PostGIS data in a GeoPandas GeoDataFrame.
        """
        if self.debug is True:
            self.info("Geoselect - %s" %sql)
        
        gdf = gpd.read_postgis(sql, self.con, geom_col=geom_col)
        
        if len(gdf) == 0:
            self.warning("... Table vide")
        
        if self.debug is True:
            self.info("... Requête executée")
        
        return gdf     

if __name__ == "__main__":        
        
    test_carto = False    
        
    # Test de la classe PgDB
    db = PgDb("configs/espace.cfg", debug=True)
    df = db.select("SELECT id_polluant, nom_abrege_polluant FROM commun.tpk_polluants WHERE id_polluant in (38, 48);")
    db.execute("DROP TABLE IF EXISTS public.toto; CREATE TABLE public.toto (id INTEGER, nom TEXT);")
    db.insert_df(df, "public", "toto", "replace")
    db.execute("INSERT INTO public.toto VALUES (8, 'téàîst'); INSERT INTO public.toto VALUES (8, '%s');" % None, accents=True)
    db.maintenance("VACUUM ANALYZE public.toto;")
    gdf = db.geoselect("SELECT * FROM cadastre.w_visu_carrieres_km;")
    db.disconnect()

    # Cartographie d'une GéoDataFrame
    if test_carto is True:
        import matplotlib
        matplotlib.use('QT4Agg')
        import matplotlib.pyplot as plt

        plt.rcParams['figure.figsize'] = (18,11)
        gdf.plot(column='val', colormap='YlOrRd', scheme='QUANTILES', k=8)
        plt.show()