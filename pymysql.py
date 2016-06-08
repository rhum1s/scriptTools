# -*- coding:utf-8 -*-
"""
PyMySQL - R. Souweine, 2016.
"""

import os
from datetime import datetime 
import ConfigParser
import MySQLdb
import pandas.io.sql as psql
import warnings
warnings.simplefilter(action = "ignore", category = FutureWarning)

class MysqlDb:
    """
    Objet pour se connecter et faire des requetes sur mysql.
    Ouvre automatiquement une connection à la création.
    """
    def __init__(self, config_file, config_section="Connection", debug=False, con=None, cur=None):
        """
        Initialisation et connection
        """        
        # Lecture de la configuration
        if os.path.isfile(config_file):
            self.config_file = config_file
            self.config_section = config_section
         
            self.config = ConfigParser.ConfigParser()
            self.config.read(config_file)
            
            self.host   = self.ConfigSectionMap(self.config, self.config_section)['host']  
            self.lgn    = self.ConfigSectionMap(self.config, self.config_section)['lgn'] 
            self.pwd    = self.ConfigSectionMap(self.config, self.config_section)['pwd'] 
            self.bdd    = self.ConfigSectionMap(self.config, self.config_section)['bdd'] 
            self.debug  = debug
        else:
            self.error("%s is doesn't exists." % config_file, exit=True)
        
        # Connection à mysql
        try:
            self.con = MySQLdb.connect(self.host, self.lgn, self.pwd, self.bdd)
            self.cur = self.con.cursor() 
        except MySQLdb.Error, e:
            error("%s" % e) 
            
        if self.debug == True:
            self.info("Connecté à %s" % self.bdd)    

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
            
    def info(self, msg):
        print "%s PgDb > %s" % (datetime.now().strftime('%Y-%m-%d %H:%M'), msg)
        
    def warning(self, msg):
        print "%s PgDb > WARNING: %s" % (datetime.now().strftime('%Y-%m-%d %H:%M'), msg)
        
    def error(self, msg, exit=False):        
        print "%s PgDb > ERROR: %s" % (datetime.now().strftime('%Y-%m-%d %H:%M'), msg)
        if exit is True:
            sys.exit()
    
    def disconnect (self):
        """
        Deconnection de mysql
        """
        try:
            self.cur.close()
            self.con.close()
        except MySQLdb.Error, e:
            error("%s" % e)         
            
    def select (self, sql, debug = False, df = True):
        """
        Execute une requete mysql de sélection.
        Si debug, ecrit la requete.
        Si df renvoie le résultat sous forme de dataframe pandas
        """
        try:
            if debug == True:
                info("Execution de la requete %s" %sql)

            if df == True:
                df = psql.frame_query(sql, con=self.con)
                if debug == True:
                    info("Requête executee")
                return df
            else:
                self.cur.execute(sql)
                if debug == True:
                    info("Requête executee")
                return self.cur.fetchall()     
                
        except MySQLdb.Error, e:
            error("%s" % e)       

    def execute(self, sql, commit = True, debug = False):
        """
        Execute une requete mysql qui ne renvoie pas de résultats.
        """        
        try:
            if debug == True:
                info("Execution de la requete %s" %sql)
            
            psql.execute(sql, con=self.con, cur=self.cur)
            self.con.commit()

            if debug == True:
                info('Requete %s exécutée.' %(self.cur.statusmessage))
                 
	except MySQLdb.Error, e:
		error('	%s' %e)

if __name__ == "__main__": 
    
    print "Testing PyMySQL ..."
    
    db = MysqlDb("configs/websig.cfg", debug=True)
    print db.select("SELECT distinct annee FROM emissions;")
    db.execute("DROP TABLE IF EXISTS toto;")
    db.disconnect()
    
    # Utilisation d'une section spécifique d'un fichier de config
    db = MysqlDb("configs/global.cfg", "websig", debug=True)
    db.disconnect()
    