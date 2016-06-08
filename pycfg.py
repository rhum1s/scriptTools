# -*- coding:utf-8 -*-
"""
PyCfg - R. Souweine, 2016.
"""
import sys
import os
import ConfigParser
from datetime import datetime

class Cfg():
    def __init__(self, cfg_file, cfg_sections, debug=False):
        """
        Lecture d'un fichier de configuation type.
        """  
        if os.path.isfile(cfg_file):
            self.cfg_file = cfg_file
            self.cfg_sections = cfg_sections
            self.config = ConfigParser.ConfigParser()
            self.config.read(cfg_file)
            self.debug = debug

            # Dictionnaire des paramètres des sections de config désirées
            config_params = {}
            for section in self.config.sections():
                if section in self.cfg_sections:
                    config_params.update(self.ConfigSectionMap(self.config, section))
 
            # Passage des paramètres en attribut de l'objet Cfg
            for key in config_params:
                setattr(self, key, config_params[key])
            
        else:
            self.error("%s is doesn't exists." % cfg_file, exit=True)
               
        if self.debug is True:
            print "Fichier de configuration lu."

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

    def show_config(self):
        parametres_caches = ("cfg_file", "cfg_sections", "config", "debug", "None")
    
        print "Paramètres de configuration"
        print "----------------------------"
        for key in self.__dict__:
            if key not in (parametres_caches) and key is not None:
                print key, "-", self.__dict__[key]                
        print ""
                   
    def info(self, msg):
        print "%s PgDb > %s" % (datetime.now().strftime('%Y-%m-%d %H:%M'), msg)
        
    def warning(self, msg):
        print "%s PgDb > WARNING: %s" % (datetime.now().strftime('%Y-%m-%d %H:%M'), msg)
        
    def error(self, msg, exit=False):        
        print "%s PgDb > ERROR: %s" % (datetime.now().strftime('%Y-%m-%d %H:%M'), msg)
        if exit is True:
            sys.exit()                
        
if __name__ == "__main__":
    
    c = Cfg("configs/global.cfg", ["inv", "general"])
    print c.show_config()