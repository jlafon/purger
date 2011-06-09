#!/usr/bin/env python
# -*- coding: utf-8 -*-

##@package purger_py
# @author Jharrod LaFon
# @date Summer 2011
#
# A simple interface to the purger database

import psycopg

##A database interface class
class PurgerDB:

    ## Default Constructor
    def __init__(self):
	    self.Database = None

    def __del__(self):
        self.disconnect()
    ##Makes a database connection
    # @param db The dataase name
    # @param username The username for accessing the database
    # @param password The password for accessing the database
    # @param hostname The server hostname with the PostgreSQL server running
    #
    def connect(self, db = 'scratch', user = 'treewalk', password = 'testing', hostname = 'localhost'):
        self.Database = pscopg.connect("host="+hostname+" dbname="+db+" user="+user+" password="+password)

    def disconnect(self):
        if self.Database != None:
            self.Database.close();
