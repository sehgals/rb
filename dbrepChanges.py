#!/usr/bin/python3
import os,time
import sys
import psycopg2
import psycopg2.extras as extras
import logging
from logging.handlers import RotatingFileHandler
import signal
import pandas as pd
import json
from datetime import datetime
from collections import OrderedDict
from multiprocessing import  Queue,Process,current_process,  Value, Lock, Event,get_start_method, freeze_support
from logging.handlers import QueueHandler
import subprocess

# Custom exception class
class DBRepConsumerError(Exception):
    pass
class DBRepProducerError(Exception):
    pass
class DBRepLogListener(object):
    # Multiprocessing log serializer to one file
    def log_configurer(self, file):
        logger = logging.getLogger()
        file_handler = RotatingFileHandler(file, maxBytes=200000, backupCount=2)
        frmt = logging.Formatter("%(asctime)s.%(msecs)03d %(processName)-10s %(message)s", "%m/%d %H:%M:%S")
        file_handler.setFormatter(frmt)
        logger.addHandler(file_handler)
        logger.setLevel(logging.INFO)
        return logger

    def log_processor(self, queue, l_configurer, file):
        lgr = l_configurer(file)
        while True:
            try:
                record = queue.get()
                if record == 'STOP':  # We send this as a sentinel to tell the listener to quit.
                    #print("Print : %s " % record)
                    break
                # print("Print : %s "%record)
                lgr.handle(record)  # No level or filter logic applied - just do it!
            except Exception:
                import sys, traceback
                print('Problem consuming log :', file=sys.stderr)
                traceback.print_exc(file=sys.stderr)

    def __init__(self,logfile):

        self.log_queue = Queue(-1)
        log_listener = Process(target=self.log_processor,args=(self.log_queue,self.log_configurer,logfile))
        log_listener.start()

    def stop_logging(self):
        self.log_queue.put('STOP')


class InfoTableConsumer(object):

    def __init__(self, queue,err_event,db_conn_dict,log_queue,info_tab_file):
        self.queue = queue
        self.err_event = err_event
        self.db_conn_dict = db_conn_dict
        self.log_queue = log_queue
        self .info_tab_file = info_tab_file
    def populate(self, tab_and_info_trg,queue,err_event):
        try:
            # As this method runs in separate process logger and database connection needs to be created here
            consumer_logger = logging.getLogger(__name__)
            consumer_logger.addHandler(QueueHandler(self.log_queue))
            consumer_logger.setLevel(logging.INFO)
            consumer_logger.info('Starting InfoTableConsumer ... ')

            consumer_logger.info('DB connection for InfoTableConsumer ... ')
            dbConn = psycopg2.connect(**self.db_conn_dict)
            dbConn.autocommit = False
            dbcur = dbConn.cursor()

            while True:
                # print('Started Consumer ... ')
                consumer_logger.debug('Running populate ... ')
                data = queue.get()

                if len(data) == 0:
                    return
                consumer_logger.debug('Got data ... ')
                table = ""
                row = ""
                table = data['table']
                ## Checking to see if producer raised an error
                if table == 'QUIT_ON_ERROR':
                    print('Inside QUIT_ON_ERROR ... ')
                    raise DBRepProducerError("DBRepProducerError event is set, existing ... ")
                row = data
                consumer_logger.debug("start")
                colNames = None
                colValues = None
                old_colNames = None
                old_colValues = None
                sqlAction = row['kind']
                newValues = "null, null"
                oldValues = "null, null"
                # New values come only when the operation is INSERT or UPDATE
                if sqlAction.upper() == 'INSERT' or sqlAction.upper() == 'UPDATE':
                    colNames = row['columnnames']
                    colValues = row['columnvalues']
                    newValues = "ARRAY%s, '%s'::jsonb"
                    newValues = newValues % (colNames, json.dumps(colValues))

                # OLD values come only when the operation is UPDATE or DELETE
                if sqlAction.upper() == 'UPDATE' or sqlAction.upper() == 'DELETE':
                    oldRow = row['oldkeys']
                    old_colNames = oldRow['keynames']
                    old_colValues = oldRow['keyvalues']
                    oldValues = " ARRAY%s, '%s'::jsonb"
                    oldValues = oldValues % (old_colNames, json.dumps(old_colValues))
                # Execute the info table stored proc
                sql_str = " select %s ( %s, %s, '%s')"
                v_sql = sql_str % (tab_and_info_trg[table], newValues, oldValues, sqlAction.upper())
                #print(v_sql)
                #consumer_logger.info(v_sql)
                consumer_logger.debug("Info Trigger SQL = %s", v_sql)
                dbcur.execute(v_sql)
                dbConn.commit()
                #time.sleep(10)
                #raise RuntimeError("Custom runtime error occurred.")
                ## TBD : remove the table entry from the info_table_file marking that record is consumed
                #        so if the process dies due error , the restart will look into the info_table_file
                #        and sync up the info table using db stored proc dbimpl.sync_info_tables()
                #        dbimpl.sync_info_tables() also needs to do update to info table based upon delete/update/insert
                #        done on main table
            dbConn.close()

        except DBRepProducerError as p_excp:
            consumer_logger.error("DBRepProducerError occurred: %s", str(p_excp))
            print('Exit InfoTableConsumer on Main error ...')
            sys.exit(1)
        except Exception as excp:
            ## Setting the error event in case this process hit an error
            err_event.set()
            consumer_logger.error("Error occurred: %s", str(excp))
            dbConn.close()
            print('Exit InfoTableConsumer ...')
            sys.exit(1)

class DBRepChanges(object):
    replConn  = None
    replcur   = None
    normConn  = None
    normcur   = None
    tablePks  = {}

    #  Helper method to read param file
    def skip_comments(self,file):
        for line in file:
            if not line.strip().startswith('#'):
                if line.strip().find('=') > 0:
                    yield line

    def set_debug_logging(self,signum, frame):
        self.logger.setLevel(logging.DEBUG)
        self.fh.setLevel(logging.DEBUG)
        print('Set log level to DEBUG...')

    def set_info_logging(self,signum, frame):
        self.logger.setLevel(logging.INFO)
        self.fh.setLevel(logging.INFO)
        print('Set log level to INFO...')

    def connect(self,params_dic):
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            self.logger.info("Wal2Dbrep connecting to the PostgreSQL database...")
            conn = psycopg2.connect(**params_dic)

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

            errmsg = "Wal2Dbrep Connecting to the PostgreSQL failure: " + str(error)
            self.logger.error(errmsg)

            # Send a trap to SS
            errtrap = "/export/home/ssuser/SOFTSWITCH/BIN/sendtrap sonusSoftSwitchWal2DbrepFailureNotification \" " + str(error) + "\" "
            os.system(errtrap)

            sys.exit(1)

        print("Connection successful")
        self.logger.info("Wal2Dbrep connection successful")
        return conn

    def columnBitsSet(self,arr):
        # As application tables have not set REPLICA IDENTITY to FULL
        # wal2json messages don't have old row information , so exact columns
        # which got updated can't be determined.
        # Setting bit mask such the all rows would appear to be updated.
        maskValue = 0
        #arr.reverse()
        for idx, val in enumerate(arr):
           maskValue = maskValue + val*pow(2,idx)
        return maskValue

    def encodeMsg(self,payload=None):
        result = None
        try:
            #self.logger.info(' --> Start Decode')
            result = json.loads(payload)
            #self.logger.info('End Json Load ')
            #result = self.decoder.decode(json_message)
            # self.logger.info(' --> End Decode ')
            # It would be expensive to cast json object to string just for logging
            if self.logger.getEffectiveLevel() == logging.DEBUG :
                resmsg = "Wal2Dbrep Json load result: " + str(result)
                self.logger.debug(resmsg)
        except UnicodeDecodeError as e:
            # Try decoding with ISO-8859-1 and re-encode with utf8
            full_msg = json.loads(payload.decode('iso-8859-1').encode('utf'))
            result = full_msg.payload
            if self.logger.getEffectiveLevel() == logging.DEBUG :
                resmsg = "Wal2Dbrep Json decoding with ISO-8859-1 and re-encode with utf8 result: " + str(result)
                self.logger.debug(resmsg)
            return result
        except Exception as excp:
            self.logger.error("Error occurred: %s", str(excp))
            raise
        return result

    def valToStr(self,typ,val):
        vRet = []
        if val == '':
            vRet.append('')
            vRet.append('~')
            return vRet

        if typ in self.pg_numeric:
            if str(val) == 'None':
                vRet.append('')
                vRet.append('~')
            else:
                vRet.append(str(val))
                vRet.append(str(len(str(val) or '')) + '~')
        elif typ[0:9] in self.pg_datetime:
            if str(val) == 'None':
                vRet.append('')
                vRet.append('~')
            else:
                try:
                    dt = datetime.strptime(val, '%Y-%m-%d %H:%M:%S.%f')
                except ValueError as val_error:
                    dt = datetime.strptime(val, '%Y-%m-%d %H:%M:%S')

                vRet.append(dt.strftime("%Y%m%d%H%M%S"))
                vRet.append(str(len(dt.strftime("%Y%m%d%H%M%S"))) + '~')
        else:
            vRet.append(str(val or '').replace("'", "\''"))
            if str(val) == 'None':
                vRet.append('~')
            else:
                vRet.append(str(len(str(val or ''))) + '~')
        return vRet

    def getTablePKS(self,conn):
        # Creating a dic of all table primary keys
        normcur = conn.cursor() #cursor_factory=RealDictCursor
        # Setting search path to main schemas
        sql = "SET search_path TO dbimpl,platform,public"
        normcur.execute(sql)
        sql = "with qry as (\
                SELECT  \
                  pg_class.oid::regclass::text tab , \
                  ARRAY[\
                  array_agg(pg_attribute.attname::text order by pg_attribute.attnum ) , \
                  array_agg(format_type(pg_attribute.atttypid, pg_attribute.atttypmod) order by pg_attribute.attnum )  ,\
                  array_agg(pg_attribute.attnum ::text order by pg_attribute.attnum ) ] arr\
                FROM pg_index, pg_class, pg_attribute, pg_namespace \
                WHERE \
                   indrelid = pg_class.oid AND \
                  nspname in ( 'dbimpl','platform') AND \
                  pg_class.relnamespace = pg_namespace.oid AND \
                  pg_attribute.attrelid = pg_class.oid AND \
                  pg_attribute.attnum = any(pg_index.indkey)\
                AND indisprimary\
                group by pg_class.oid::regclass\
                )\
                select json_object_agg( tab ,arr)  from qry"
        normcur.execute(sql)
        jsonDoc = normcur.fetchone()
        self.tablePks = dict(jsonDoc[0])

    def sync_info_tables(self):
        pass
    ##   TBD call dbimpl.sync_info_tables() here

    def create_val_offset_lists(self,col_types,col_values,val_list,offset_list):
        for idx, typ in enumerate(col_types):
            retVal = self.valToStr(typ, col_values[idx])
            val_list = val_list + retVal[0]
            offset_list = offset_list + retVal[1]
        return val_list,offset_list
    def prepare_row_upd_lists(self, colNames, colTypes, colValues, oldkeys, valList, offsetList, bitArray):
        ## All columns are not reported in OldKeys
        ## Specially not modified , nullable cols are not reported.
        ## We need all old cols picture, so that we can find the diff
        ## when nullable cols with null value in it is changed
        for idx, col in enumerate(colNames):
            # If the col exists in oldkey compare old and new value
            typ = colTypes[idx]
            if col in oldkeys['keynames']:
                old_col_index = oldkeys['keynames'].index(col)
                old_col_value = oldkeys['keyvalues'][old_col_index]
            else:
                old_col_value = None

            if old_col_value != colValues[idx]:
                retVal = self.valToStr(typ, colValues[idx])
                valList = valList + retVal[0]
                offsetList = offsetList + retVal[1]
                bitArray[idx] = 1
        return (valList,offsetList)

    def __init__(self,pwd,db_params,queue,err_event,log_queue,info_trg_map,info_view_map,info_tab_file):
        dbrep_logger = logging.getLogger("DBRepChanges")
        dbrep_logger.addHandler(QueueHandler(log_queue))
        dbrep_logger.setLevel(logging.INFO)

        self.logger = dbrep_logger
        self.ssuser_passwd = pwd
        self.info_table_queue = queue
        self.info_table_err_event = err_event
        self.info_tab_file = info_tab_file
        repl_conn_param_dic = {
            "host": db_params["DbIpAddress"],
            "user": "postgres",
            "dbname": "ssdb",
            "port": "5432",
            "password": "",
            "application_name": "wal2dbrep.service",
            "connection_factory": psycopg2.extras.LogicalReplicationConnection
        }

        conn_param_dic = {
            "host":  "127.0.0.1",
            "port": "5432",
            "dbname": "ssdb",
            "user": "ssuser",
            "password": self.ssuser_passwd,
            "application_name": "wal2dbrep.service",
            "connection_factory": None
        }


        self.pg_numeric = ['smallint', 'integer', 'bigint', 'decimal', 'numeric', 'real', 'double precision', 'smallserial',
                      'serial', 'bigserial']
        self.pg_datetime = ['timestamp', 'date', 'time', 'interval']
        self.bulk_tables = ['vbr_vendor_rate_sheet_data']

        self.v_sql_dbrep_sqlid = "INSERT INTO platform.dbrep_changes_sqlids(change_id,sql_id)VALUES(%d,%d)"
        self.v_sql_dbrep = "INSERT INTO platform.dbrep_changes(change_id,userid,time,owner,table_name,sql_type, \
                        sql_values,sql_value_offsets,upd_cols,status,lob_change_id,partition_id) VALUES (%d,'%s','%s','%s','%s','%s','%s','%s',%d,%d,%d,%d)"

        # A map to map table and info trigger
        self.tableAndInfoTrigger = info_trg_map
        self.info_table_view = info_view_map
        self.logger.info('DB connection for DbRepChanges repl slot ... ')
        self.replConn = self.connect(repl_conn_param_dic)
        self.replcur = self.replConn.cursor()
        self.logger.info('DB connection for DbRepChanges normal processing ... ')
        self.normConn = self.connect(conn_param_dic)
        self.normConn.autocommit = False
        self.normcur = self.normConn.cursor()
        self.info_table_dict = OrderedDict()
        self.getTablePKS(self.normConn)

        replOptions = {'add-tables': 'dbimpl.*,platform.table_changes',
                       'filter-tables': 'dbimpl.hosted_lnp',
                       'include-pk': 'true',
                       'format-version' : '1'}

        # Send a trap to SSA
        starttrap = "/export/home/ssuser/SOFTSWITCH/BIN/sendtrap sonusSoftSwitchWal2DbrepConnectNotification \"Wal2Dbrep connection UP\" "
        os.system(starttrap)


        # Setup Signalling
        # if process receives SIGUSR1 signal, the logging level changes to DEBUG.
        signal.signal(signal.SIGUSR1, self.set_debug_logging)

        # if process receives SIGUSR1 signal, the logging level changes to INFO.
        signal.signal(signal.SIGUSR2, self.set_info_logging)
        try:
            self.logger.info("Wal2Dbrep start time : %s ", datetime.now().strftime("%Y-%m-%d %H:%M:%S") )

            ## TBD sync up info table from previous failure
            ## Check info_table_file for this
            ## call self.sync_info_tables()

            # test_decoding produces textual output
            self.replcur.start_replication(slot_name='orarepl', decode=False, status_interval=60, options=replOptions)

        except psycopg2.ProgrammingError:

            self.logger.info("Wal2Dbrep loads wal2json plugin")

            self.replcur.create_replication_slot('orarepl', output_plugin='wal2json')
            self.replcur.start_replication(slot_name='orarepl', decode=False, status_interval=60, options=replOptions)

    def __call__(self, msg):
        #print(msg.payload)
        try:
            ## Checking to see if consumer raised an error
            if self.info_table_err_event.is_set():
                print("Error event is set, existing ....")
                self.logger.info("Error event is set, existing ....")
                raise DBRepConsumerError("Consumer error event is set, existing ... ")
            if msg.data_size <= 25:
                #self.logger.info("Empty payload received, returning ... ")
                msg.cursor.send_feedback(flush_lsn=msg.data_start)
                self.normConn.commit()
                return

            data = self.encodeMsg(msg.payload)
            num_recs_in_msg = len(data['change'])
            # Short-circuiting single message of type 'message' or bdr messages
            if num_recs_in_msg <= 1:
                if num_recs_in_msg == 0 or data['change'][0]['kind'] == 'message' or data['change'][0]['schema'] == 'bdr':
                    #print("Message end time : %s ", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    self.logger.debug("Wal2Dbrep Message end time")
                    msg.cursor.send_feedback(flush_lsn=msg.data_start)
                    self.normConn.commit()
                    return

            pd.set_option('display.max_columns', None)
            df = pd.DataFrame(data['change'])

            table = ''
            whereCondition = ''
            bulkReplMesg = False
            #print("Message start time : %s ", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            self.logger.debug("Wal2Dbrep Message start time : %s ", datetime.now().strftime("%Y-%m-%d %H:%M:%S") )
            self.info_table_dict.clear()
            for index, row in df.iterrows():
                whereCondition = ''
                bulkReplMesg = False
                dtLoaded = datetime.now()
                valList = ''
                offsetList = ''
                sqlAction = row['kind']
                bitArray = [0]
                # Ignore BDR Messages
                if sqlAction != 'message':
                    owner = row['schema']
                    if owner != 'bdr':
                        v_sql = ''
                        if (owner == 'dbimpl' or owner == 'platform'):
                            table = row['table']
                            if ( table not in self.bulk_tables ):
                                if ( owner == 'platform' and table not in ['table_changes','table_changes_data'] ):
                                    print("Platform table %s is not replicated" %(table))
                                    callmsg = "Platform table " + str(table) + " is not replicated"
                                    self.logger.debug(callmsg)
                                else:
                                    if row['kind'] in ['insert', 'update']:
                                        #colList = '('
                                        colNames = row['columnnames']
                                        colTypes = row['columntypes']
                                        colValues = row['columnvalues']
                                        # Array created for all cols in table
                                        bitArray = [0] * len(colTypes)
                                        # This will be used in update only
                                        if row['kind'] == 'update':
                                            # Processing the primary keys
                                            tablePkData = self.tablePks.get(table)
                                            if tablePkData:
                                                oldkeys = row['oldkeys']
                                                # tablePkData[1] holds pk cols datatype
                                                # tablePkData[0] hold pk cols names
                                                primaryKeysCols = tablePkData[0]
                                                primaryKeysColTypes = tablePkData[1]
                                                primaryKeysColValues =[]
                                                # tablePkData[2] hold pk col position, looping over position to get the pk values before update
                                                for key in primaryKeysCols:
                                                    posi = oldkeys['keynames'].index(key)
                                                    primaryKeysColValues.append(oldkeys['keyvalues'][posi])

                                                # Appending the primary keys to the beginning of string
                                                valList, offsetList = self.create_val_offset_lists(primaryKeysColTypes, primaryKeysColValues, valList,
                                                                        offsetList)
                                                # Processing other columns
                                                valList, offsetList = self.prepare_row_upd_lists(colNames, colTypes, colValues, oldkeys,
                                                                                                 valList, offsetList, bitArray)

                                        else:
                                            # Processing the table columns for insert
                                            valList, offsetList = self.create_val_offset_lists(colTypes,
                                                                                               colValues,
                                                                                               valList,
                                                                                               offsetList)

                                    elif row['kind'] == 'delete':
                                        tablePkData = self.tablePks.get(table)
                                        if tablePkData:
                                            oldkeys = row['oldkeys']
                                            # tablePkData[1] holds pk cols datatype
                                            # tablePkData[0] hold pk cols names
                                            primaryKeysCols = tablePkData[0]
                                            primaryKeysColTypes = tablePkData[1]
                                            primaryKeysColValues = []
                                            # tablePkData[2] hold pk col position, looping over position to get the pk values before update
                                            for key in primaryKeysCols:
                                                posi = oldkeys['keynames'].index(key)
                                                primaryKeysColValues.append(oldkeys['keyvalues'][posi])

                                            # Appending the primary keys to the beginning of string
                                            valList, offsetList = self.create_val_offset_lists(primaryKeysColTypes,
                                                                                               primaryKeysColValues,
                                                                                               valList,
                                                                                               offsetList)
                            else:
                                # Create bulk replication message or replicate row by row depending on batch_seq_num
                                # batch_seq_num will not be NULL only if ratesheet is loaded through lcrrsldr tool
                                # batch_seq_num is not set when entering data through UI or CLI
                                # Bulk replication delete message is directly entered by api into dbrep_changes
                                # Bulk updates are not supported
                                if row['kind'] == 'insert':
                                    #colList = '('
                                    colNames = row['columnnames']
                                    colTypes = row['columntypes']
                                    colValues = row['columnvalues']
                                    bitArray = [0] * len(colTypes)
                                    ratesheetIdIndx = colNames.index('ratesheet_id')
                                    batchSeqNumIndx = colNames.index('batch_seq_num')
                                    ratesheetId = colValues[ratesheetIdIndx]
                                    batchSeqNum = colValues[batchSeqNumIndx]
                                    #if batchSeqNum not NULL, then do bulk replication based on batch_seq_num
                                    #if  batchSeqNum is NULL, then replication is done row by row
                                    if batchSeqNum:
                                        if table == 'vbr_vendor_rate_sheet_data':
                                            vendorIdIndx = colNames.index('vendor_id')
                                            vendorId = colValues[vendorIdIndx]
                                            whereCondition = " WHERE vendor_id = '%s' AND ratesheet_id ='%s' AND batch_seq_num = %s " % (vendorId, ratesheetId, batchSeqNum)
                                        if table == 'vbr_offer_rate_sheet_data':
                                            offerIdIndx = colNames.index('offer_id')
                                            offerId = colValues[offerIdIndx]
                                            whereCondition = " WHERE offer_id = '%s' AND ratesheet_id ='%s' AND batch_seq_num = %s " % (offerId, ratesheetId, batchSeqNum)
                                        bulkReplMesg = True
                                        break
                                    else :
                                        # Processing the table columns
                                        # for idx, typ in enumerate(colTypes):
                                        #     retVal = self.valToStr(typ, colValues[idx])
                                        #     valList = valList + retVal[0]
                                        #     offsetList = offsetList + retVal[1]
                                        #     bitArray[idx]= 1
                                        valList, offsetList = self.create_val_offset_lists(colTypes,
                                                                                           colValues,
                                                                                           valList,
                                                                                           offsetList)

                                elif row['kind'] == 'update':
                                    #Updates are only throught UI or CLI, so replication is done row by row
                                    colNames = row['columnnames']
                                    colTypes = row['columntypes']
                                    colValues = row['columnvalues']
                                    bitArray = [0] * len(colTypes)
                                    # Processing the primary keys
                                    tablePkData = self.tablePks.get(table)
                                    oldkeys = row['oldkeys']
                                    # tablePkData[1] holds pk cols datatype
                                    # tablePkData[0] hold pk cols names
                                    primaryKeysCols = tablePkData[0]
                                    primaryKeysColTypes = tablePkData[1]
                                    primaryKeysColValues =[]
                                    # tablePkData[2] hold pk col position, looping over position to get the pk values before update
                                    for key in primaryKeysCols:
                                        posi = oldkeys['keynames'].index(key)
                                        primaryKeysColValues.append(oldkeys['keyvalues'][posi])

                                    # Appending the primary keys to the beginning of string
                                    valList, offsetList = self.create_val_offset_lists(primaryKeysColTypes,
                                                                                       primaryKeysColValues,
                                                                                       valList,
                                                                                       offsetList)
                                    valList, offsetList = self.prepare_row_upd_lists(colNames, colTypes, colValues,
                                                                                     oldkeys, valList, offsetList,
                                                                                     bitArray)

                        if valList:

                            if row['kind'] == 'update':
                                updCols = self.columnBitsSet(bitArray)
                                # If there are no actual updated cols then skip this update as offset string will not have values.
                                if updCols == 0:
                                    #break
                                   continue
                            else :
                                updCols = 0

                            self.normcur.execute("select nextval('platform.dbrep_changeid_seq')")
                            id = self.normcur.fetchone()
                            v_sql = self.v_sql_dbrep_sqlid % (id[0], 0)
                            self.normcur.execute(v_sql)

                            v_sql = self.v_sql_dbrep % (id[0], 'ssuser', dtLoaded, owner.upper(), table.upper(),
                                                   sqlAction.upper(), valList, offsetList, updCols, 1, 0, int(dtLoaded.day))
                            self.normcur.execute(v_sql)
                            self.logger.debug(" Table  = %s", table.upper())
                            self.logger.debug(" SQL Operation = %s", row['kind'])

                            # Collecting data in Ordered dict for info table processing
                            if table in self.tableAndInfoTrigger.keys():
                                self.info_table_dict[table+f"###_{index}"] = row
                            ## Test to create error in main process
                            # if index == 5:
                            #     print( 'Index 5 error raised .... ')
                            #     raise Exception("Error in DBRepChanges runtime error occurred.")

                #self.replcur.send_feedback(reply=True)
                self.replcur.send_feedback(write_lsn=msg.data_start,flush_lsn=msg.wal_end,reply=True)
                # normConn.commit()

            # print(msg.payload)
            if bulkReplMesg and whereCondition:
                # Bulk Replication message
                statusMsg = ''
                statusCode = 0
                self.normcur.execute("call dbimpl.bulk_repl_mesg(%s,%s,%s,%s);", (table, whereCondition, statusMsg, statusCode))
                # normcur.callproc('dbimpl.bulk_repl_mesg',[table, whereCondition,statusMsg,statusCode])
                id = self.normcur.fetchone()
                print("Return Message : %s - Return Code: %s ", id[0], id[1])
                callmsg = "Return Message : %s - Return Code: %s " % (id[0], id[1])
                self.logger.debug(callmsg)

                self.normConn.commit()

            #print("Message end time : %s ", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            self.logger.debug("Wal2Dbrep Message end time")

            ## Processing the info table ordered dict
            #  As queue consumer commits every record it is safer to process
            #  all info table records in the transaction after all parent table
            #  records are processed.

            self.populateInfoTabProducer(self.info_table_dict)
            self.info_table_dict.clear()

            msg.cursor.send_feedback(flush_lsn=msg.data_start)
            self.normConn.commit()
            #self.replcur.send_feedback(write_lsn=msg.data_start,flush_lsn=msg.wal_end,force=True)
            msg.cursor.send_feedback(flush_lsn=msg.data_start,reply=True)

        except DBRepConsumerError as c_excp:
            ## exiting because info table consumer errored.
            self.logger.error("Error occurred: %s", str(c_excp))
            raise
        except Exception as excp:
            ## Setting the err event so that info table consumer will exit
            ## as main process hit an error
            self.info_table_err_event.set()
            self.info_table_queue.put({'table': 'QUIT_ON_ERROR'})
            self.logger.error("Error occurred: %s", str(excp))
            raise
    def populateInfoTabProducer(self,info_dict):
        for key, val in info_dict.items():
            self.info_table_queue.put(val)
            ## After queing write the table_name to a file
            ## We want to mimic persistent queue
            ## TBD

def get_ssuser_passwd(logger):
    ssuser_passwd = "ssuser"
    pid = os.getpid()
    passwd_cmd = "su - ssuser -c 'pass_decrypt.sh " + str(pid) + "'"
    passwd_file = '/export/home/ssuser/tmp/pass.' + str(pid)
    ssuser_file = '/export/home/ssuser/pstore/passwords/ssuser.gpg'

    if os.path.isfile(ssuser_file):
        os.system(passwd_cmd)

        # print('Open DB password file ' + passwd_file)
        if os.path.isfile(passwd_file):
            with open(passwd_file, 'r') as f:
                # print('Take DB password from ' + passwd_file)
                logger.info("Wal2Dbrep: use the password from the Secure Storage...")
                ssuser_passwd = f.read()
                # print('Read DB password: ' + ssuser_passwd)
                f.close()
        else:
            print('No DB password file, using default')
            logger.info("Wal2Dbrep: fails to take the password from the Secure Storage, use default...")

    else:
        print('Use default DB password')
        logger.info("Wal2Dbrep: Use default DB password")

def main():
    #w2d_logfile = "wal2dbrep.log"
    info_table_processing_list = "info_table_processing_list.txt"
    w2d_logfile = "/var/log/postgres/wal2dbrep.log"


    # It was observed that sometimes the log file owned by root and postgres is not able to write to it.
    # So changing the permission
    # Postgres is given permission via /etc/sudoers to change permission on the file.
    cmd = f'sudo /usr/bin/chown postgres:dba {w2d_logfile}'
    try:
       p = subprocess.run(cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except Exception as err:
       print(err)
    log_listener = DBRepLogListener(w2d_logfile)

    queue_handler = QueueHandler(log_listener.log_queue)
    main_logger = logging.getLogger(__name__)
    main_logger.addHandler(queue_handler)
    main_logger.setLevel(logging.INFO)

    ssuser_passwd =  get_ssuser_passwd(main_logger)
    paramFile = '/var/opt/sonus/ssScriptInputs'
    # Parsing the parameter file
    with open(paramFile,"r") as f:
        d = {k: v[1:-2] for k, v in (l.split('=', 1) for l in self.skip_comments(f))}

    #"172.19.52.102"
    d = {'DbIpAddress': "172.19.52.102"}
    process_conn_param_dic = {
        "host": d["DbIpAddress"],  # "127.0.0.1", #
        "port": "5432",
        "dbname": "ssdb",
        "user": "ssuser",
        "password": ssuser_passwd,
        "application_name": "wal2dbrep.service.infotable",
        "connection_factory": None
    }
    info_table_procs = {'route': 'dbimpl.upd_route_info_pri_dbrep',
                        'authcode': 'dbimpl.upd_authcode_info_trg_dbrep',
                        'blocking_label_profile': 'dbimpl.blocking_label_profile_dbrep',
                        'calling_area': 'dbimpl.upd_calling_area_info_trg_dbrep',
                        'carrier_dest_chg_ind': 'dbimpl.upd_carrier_dest_chg_ind_info_trg_dbrep',
                        'charge_band_profile': 'dbimpl.upd_charge_band_profile_info_trg_dbrep',
                        'destination': 'dbimpl.upd_destination_info_trg_dbrep',
                        'dpc_information': 'dbimpl.upd_dpc_information_info_trg_dbrep',
                        'escaped_number': 'dbimpl.escaped_number_info_trg_dbrep',
                        'lawful_int_crit_data': 'dbimpl.upd_lic_info_trg_dbrep',
                        'non_subscriber': 'dbimpl.upd_non_subscriber_info_trg_dbrep',
                        'npa_nxx': 'dbimpl.upd_npa_nxx_info_trg_fn_dbrep',
                        'number_control_profile_data': 'dbimpl.upd_number_control_info_trg_dbrep',
                        'number_translation_data': 'dbimpl.upd_number_trans_data_info_trg_dbrep',
                        'subscriber': 'dbimpl.subscriber_sub_info_trg_dbrep',
                        'tollfree_prefix': 'dbimpl.upd_tollfree_prefix_info_trg_dbrep',
                        'vpn_subscriber': 'dbimpl.upd_vpn_subscriber_info_trg_dbrep',
                        'vbr_customer_identity': 'dbimpl.upd_vbr_customer_id_info_trg_dbrep'}

    info_table_queue = Queue()
    info_table_err_event = Event()

    repChanges = DBRepChanges(pwd=ssuser_passwd, db_params=d, queue=info_table_queue,
                              err_event=info_table_err_event,log_queue=log_listener.log_queue,
                              info_trg_map=info_table_procs,
                              info_tab_file=info_table_processing_list)

    info_table_consumer = InfoTableConsumer(queue=info_table_queue, err_event=info_table_err_event,
                                 db_conn_dict=process_conn_param_dic,log_queue=log_listener.log_queue,
                                            info_tab_file=info_table_processing_list)

    info_table_consumer_prcs = Process(target=info_table_consumer.populate, name='populateInfoProcess',
                                              args=(info_table_procs,info_table_queue,info_table_err_event,))

    info_table_consumer_prcs.start()

    print("Starting streaming, press Control-C to end...", file=sys.stderr)
    main_logger.info("Starting streaming, press Control-C to end...")
    try:
        repChanges.replcur.consume_stream(repChanges, 10)

    except KeyboardInterrupt:
        repChanges.replcur.close()
        repChanges.replConn.close()
        repChanges.normcur.close()
        repChanges.normConn.close()
        info_table_queue.put({'table': 'QUIT_ON_ERROR'})
        info_table_consumer_prcs.join()
        log_listener.stop_logging()
        logging.shutdown()
        sys.exit(1)
    except Exception as exp:
        repChanges.replcur.close()
        repChanges.replConn.close()
        repChanges.normcur.close()
        repChanges.normConn.close()
        print("Main process encountered an error:", exp)
        print(
            "The slot 'orarepl' still exists. Drop it with SELECT pg_drop_replication_slot('orarepl'); if no longer needed.",
            file=sys.stderr)
        print("WARNING: Transaction logs will accumulate in pg_xlog until the slot is dropped.", file=sys.stderr)
        # main_logger.info("The slot 'orarepl' still exists. Drop it with SELECT pg_drop_replication_slot('orarepl'); if no longer needed.")
        # main_logger.info("WARNING: Transaction logs will accumulate in pg_xlog until the slot is dropped.")
        # main_logger.info("Main process encountered an error:", exp)
        # self.logger.warning("Wal2Dbrep finished")
        info_table_consumer_prcs.join()
        log_listener.stop_logging()
        logging.shutdown()
        print('Exit Main .... ')
        sys.exit(1)


if __name__ == "__main__":
    # Add Multiprocessing freeze_support() for Windows to avoid the RuntimeError
    # if get_start_method() == 'spawn':
    #     freeze_support()
    main()
