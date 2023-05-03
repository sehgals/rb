#!/usr/bin/python3


import ruamel.yaml
import sys, os
import glob
import argparse
import pandas as pd
from pathlib import Path
import csv
import subprocess
import logging
import mmap
import traceback
import numpy as np
from multiprocessing import Pool
import time


dump_flag = ''
pg_datatype_conv = {
    'smallint': 'Int64',
    'integer': 'Int64',
    'bigint': 'Int64',
    'decimal': 'Float64',
    'numeric': 'Float64',
    'real': 'Float64',
    'double precision': 'Float64',
    'smallserial': 'Int64',
    'serial': 'Int64',
    'bigserial': 'Int64',
    'timestamp': 'datetime64[ns]', 'date': 'datetime64[ns]', 'time': 'datetime64[ns]', 'interval': 'Int64',
    'timestamp without time zone': 'datetime64[ns]',
    'text': 'str', 'varchar': 'str', 'char': 'str', 'character': 'str', 'timestamp with time zone': 'string',
    'boolean': 'boolean', 'bytea': 'string', 'time with time zone': 'string', 'time without time zone': 'string',
}
# Skip tables
skip_table =['REST_REQ_BODY_PROFILE','PSX_CSD_INFO']
# custom_date_parser = lambda x: dt.strptime(x, "%d-%b-%Y %H:%M:%S")
custom_date_parser = lambda x: (
    pd.to_datetime(x, format="%d-%b-%Y %H:%M:%S", errors='ignore') if not pd.isna(x) else pd.NaT)


def processKeys(keys, data):
    for key in keys:
        #
        # print("=== Processing : "+key)
        for path, value in lookupYaml(key, data):
            yield (path, value)


def mapcount(filename):
    f = open(filename, "r+")
    buf = mmap.mmap(f.fileno(), 0)
    lines = 0
    readline = buf.readline
    while readline():
        lines += 1
    return lines


def lookupYaml(skey, data, path=[]):
    # lookupYaml the values for key(s) skey return as list the tuple (path to the value, value)
    # print( "Calling lookupYaml :"+skey+" : "+str(data))
    if isinstance(data, dict):
        for k, v in data.items():
            # print("k ="+ str(k) + " v = "+str(v)+ " Type = "+str(type(v)))
            if k == skey:
                # print("Data = "+str(data))
                if skey == "index":
                    # print("1 Skey == "+ skey + " K ="+k+ " Data == "+ str(v) +str(path) )
                    if "na" == v.get("tablespace", "na"):
                        if "members" in path:
                            # For columns with index under members  we come here
                            # if we reached here that means tablespace for indexed column is missing
                            # print("15 -- Updating tablespace .....")
                            v["tablespace"] = "rteidx"
                        else:
                            # print(" Here 1")
                            for res in lookupYaml("tablespace", v, path + [k]):
                                yield res
                        # print("Adding tablespace to index")
                    else:
                        # print("Here 2")
                        if v.get("tablespace", "na") != "rteidx":
                            ##print("2 Updating tablespace .....")
                            v.update({'tablespace': "rteidx"})
                else:
                    # print("Other Top Skey == "+ skey + " K ="+k)
                    # print("1 Skey == "+ skey + " K ="+k+ " Data == "+ str(v) +str(path) )
                    if k == "tablespace" and v != "rteidx" and ("index" in path or "primary_key" in path):
                        ##print("10 -- Updating tablespace .....")
                        data["tablespace"] = "rteidx"
                    elif k == "index" and v != "rteidx" and "index" in path:
                        ##print("11 -- Updating tablespace .....")
                        v.update({'tablespace': "rteidx"})
                    elif v == "tablespace" and v != "rteidx" and "database_properties" in path:
                        ##print("12 -- Updating tablespace .....")
                        v == "rteidx"
                    elif k == "primary_key":
                        if "na" == v.get("tablespace", "na") and "members" in path:
                            # For columns with pk under members  we come here
                            # if we reached here that means tablespace for pk column is missing
                            ##print("14 -- Updating tablespace .....")
                            v["tablespace"] = "rteidx"
                        elif "na" == v.get("tablespace", "na") and "database_properties" in path:
                            # For columns with pk under database_properties  we come here
                            # if we reached here that means tablespace for pk column is missing
                            ##print("16 -- Updating tablespace .....")
                            v["tablespace"] = "rteidx"
                        else:
                            # Look for tablespace under primary key
                            ##print("13 -- Updating tablespace .....")
                            for res in lookupYaml("tablespace", v, path + [k]):
                                yield res
                    else:
                        for res in lookupYaml(k, v, path + [k]):
                            yield res

                    yield (path + [k], v)
            for res in lookupYaml(skey, v, path + [k]):
                yield res
    elif isinstance(data, list):
        for item in data:
            for res in lookupYaml(skey, item, path + [item]):
                yield res


def fillColDefValues(table, col, col_value, df, flag):
    if col_value:
        if flag == 'pg':
            col = col.lower()
        # Special processing for country table
        if (table == 'COUNTRY' and col.lower() == 'erp_profile_id'):
            df[col] = df[col].fillna("DEFAULT_IP")
        else:
            df[col] = df[col].fillna(col_value)


def processTmpFiles(chunkFiles, targetFile):
    # Concatinating chunk files with one header line
    first_file = True
    for file in chunkFiles:
        fn = list(file.keys())[0]
        # Modifying the header
        with open(fn, 'r', encoding='utf-8') as r_file:
            lines = r_file.readlines()
            lines[0] = lines[0].replace('"', '')
            # Replace "\N" with \N for non numeric cols
            for line in lines:
                line = line.replace('\"\\N\"', '\\N')

        if first_file:
            with open(targetFile, 'w', encoding='utf-8') as w_file:
                w_file.writelines(lines)
        else:
            with open(targetFile, 'a', encoding='utf-8') as w_file:
                # Dropping the header
                w_file.writelines(lines[1:])
        first_file = False
        os.remove(fn)

    # Move the modified file to target dir
    # _tmp is the /orig
    if os.path.exists(f"{in_data_dump_folder_tmp}/{datafilenm}"):
        os.remove(Path(f"{in_data_dump_folder_tmp}/{datafilenm}"))
    # Moving data csv file into ../orig
    Path(datafile).rename(Path(f"{in_data_dump_folder_tmp}/{datafilenm}"))
    # Move the modified data file to data dir
    Path(targetFile).rename(f"{datafile}")
    if os.path.exists(f"{datafile}") and os.name == 'posix':
        cmd = f"chown postgres:postgres {datafile} "
        p = subprocess.run(cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # print("Done")

def processChunk(chnk):
    #n,chnk,cols,int_cols,float_cols,date_cols,df_reqd_cols_def, dataTempFldr, flag, tmp_files
    n=chnk[0]
    d = chnk[1]
    cols = chnk[2]
    int_cols = chnk[3]
    float_cols = chnk[4]
    date_cols = chnk[5]
    df_reqd_cols_def = chnk[6]
    dataTempFldr = chnk[7]
    flag = chnk [8]
    tmp_files = chnk[9]
    header_cols = chnk[10]
    col_nm_col = chnk[11]
    file_name = chnk[12]
    tablenm = chnk[13]
    data_def_col = chnk[14]

    dfChunk = pd.DataFrame(data=chnk[1], columns=chnk[2])
    for column in dfChunk:
        if (column in int_cols) or (column in float_cols):
            # dfChunk[column] = dfChunk[column].map({'\\N': np.NaN})
            dfChunk.loc[dfChunk[column] == '\\N', column] = np.NaN
        elif column in date_cols:
            dfChunk.loc[dfChunk[column] == '\\N', column] = pd.NaT

    dfChunk = dfChunk.astype(dtype=header_cols, errors='ignore')

    ###################
    if len(dfChunk.index) > 0:
        df_tbl_data_reqd_cols = dfChunk[
            [c for c in dfChunk.columns if c.upper() in df_reqd_cols_def[col_nm_col].values]]
        c = df_tbl_data_reqd_cols.columns[df_tbl_data_reqd_cols.isna().any()]

        datafilenm = os.path.basename(file_name)
        moddatafile = Path(f"{dataTempFldr}/m_{datafilenm}_{n}")

        # If any reqd columns have NA values
        # if len(c) >= 0:
        df_reqd_cols_def_fil = df_reqd_cols_def[df_reqd_cols_def[col_nm_col].str.lower().isin(c)]
        # print(f"File {datafile}: Has null values in required column with default values")
        ## Save the original file ../orig folder

        # Remove chunk file
        if os.path.exists(moddatafile):
            os.remove(moddatafile)

        result = [fillColDefValues(tablenm, col, col_value, dfChunk, flag) for col, col_value in
                  zip(df_reqd_cols_def_fil[col_nm_col], df_reqd_cols_def_fil[data_def_col])]

        dfChunk.to_csv(moddatafile, index=False, sep=',', quoting=csv.QUOTE_NONNUMERIC, quotechar='"', na_rep='\\N',
                       doublequote=False,
                       escapechar='\\', encoding='utf-8', date_format="%d-%b-%Y %H:%M:%S")
        # Converting "\N" --> \N
        with open(moddatafile, 'r', encoding='utf-8') as r_file:
            data = r_file.read()

        if data:
            with open(moddatafile, 'w', encoding='utf-8') as w_file:
                data = data.replace('\"\\N\"', '\\N')
                w_file.write(data)

            if len(c) > 0:
                return {moddatafile: 1}
            else:
                return {moddatafile: 0}
        else:
            # print(f"File {datafile}: is ok")
            None

def generateChunk(reader, chunksize):
    chunk = []
    for i, line in enumerate(reader):
        if (i % chunksize == 0 and i > 0):
            yield chunk
            del chunk[:]  # or: chunk = []

        chunk.append(line)
    yield chunk

def modifyDataFile(datafile, reqdcols, yamlCols, dataTempFldr, flag,tabName,data_def_col):  # df_reqd_cols,cols
    if os.path.exists(datafile) and len(reqdcols.index) > 0:
        pd.set_option('display.max_columns', None)
        # header_cols = pd.read_csv(datafile,encoding_errors='ascii', nrows=0).to_dict()
        with open(datafile, 'r', errors='replace') as r_file:  # , encoding='utf-8'
            line = r_file.readline()
            header_cols = dict((item, "") for item in line.strip().split(","))
        # print(f" fn = {datafile} ")
        date_cols = []
        int_cols = []
        float_cols = []
        for col in header_cols:
            # print(f"col --> {col}")
            try:
                header_cols[col] = pg_datatype_conv[yamlCols[col.lower()]['type']]
                col_type = yamlCols[col.lower()]['type']
                if 'timestamp' in col_type:
                    date_cols.append(col)
                if 'int' in col_type.lower():
                    int_cols.append(col)
                if 'numeric' in col_type.lower():
                    float_cols.append(col)
            except KeyError as k:
                # We will come when cols --> are primary keys and col --> all columns in csv
                None
        # chunksize in lines
        chunksize = 1000000
        n = 0  # File number per chunk
        # Identify required columns with default value
        # data_def_col is read from context
        df_reqd_cols_def = reqdcols[reqdcols[data_def_col].notnull()]
        cols = list(df_reqd_cols_def[col_nm_col].str.lower())
        tmp_files = {}

        # If any required columns have default value specified in application_tab_cols
        if len(cols) > 0:
            try:
                with open(datafile, 'r', errors='replace') as r_file:
                    cols = [i for i in header_cols.keys()]
                    col_dtypes = [i for i in header_cols.values()]
                    logging.info(f"Validating/Modifying data file {datafile}")
                    #reader = csv.DictReader(r_file,dialect='unix', delimiter = ',',quotechar='"',escapechar='\\')
                    reader = csv.reader(r_file, dialect='unix', delimiter=',',quotechar='"')
                    #Skipping header
                    next(reader)
                    chunks = []
                    n = 0
                    for chnk in generateChunk(reader, chunksize):
                        # Go back to pandas when pandas are upgraded from 1.1 to 1.5.3
                        # for dfChunk in pd.read_csv(datafile, header=0, chunksize=chunksize,dtype=header_cols,na_values=['\\N','<NA>'],
                        #                       date_parser=custom_date_parser,parse_dates=date_cols,encoding_errors='replace'):
                        n = n + 1
                        chunks.append([n,chnk,cols,int_cols,float_cols,date_cols,df_reqd_cols_def, dataTempFldr, flag, tmp_files,header_cols,col_nm_col,datafile,tabName,data_def_col])
                    #print("Totals Chunks : ",len(chunks))

                # Rules to define number of processes in the pool
                # default value
                num_processes = 2
                cpus = os.cpu_count()
                num_chunks = len(chunks)
                if (cpus <= 2) or (num_chunks == 1) :
                    num_processes = 1
                elif cpus > 2 and  num_chunks > 1:
                    # Number processes should not be greater than cpus -2
                    # Adjust the processes for small number of chunks
                    num_processes = cpus -2
                    if num_processes >= num_chunks:
                        # Taking the lower value of calculated processes and available chunks
                        num_processes = num_chunks
                p = Pool(num_processes)
                tmp_files = p.map(processChunk, chunks)
                if len(tmp_files) > 0:
                    ### If any of the chunks had data correction then recreate the whole file
                    process_file= False
                    for file in tmp_files:
                        if list(file.values())[0] > 1:
                            process_file = True
                            break
                    if process_file :
                        processTmpFiles(tmp_files, Path(f"{dataTempFldr}/m_{datafilenm}"))
                    else :
                        #Remove temp files
                        files = glob.glob(f"{in_data_dump_folder_tmp}/m_{datafilenm}*")
                        for f in files:
                            os.remove(f)
            except Exception:
                print(traceback.format_exc())
    else:
        # print(f"Skipping file {datafile} ...")
        None


def applyRequired(cols, chkpk, file, df, data_folder, data_tmp, flag):
    # This method checks whether table column is part of primary key
    # and inserts required = yes for it.

    # Loop over columns defined in csd yaml file
    for k, v in cols.items():
        if chkpk:
            if 'primary_key' in v:
                if 'required' in v:
                    cols[k]['required'] = 'yes'
                else:
                    # print('Fixing PK required yes in ',file)
                    cols[k].update({'required': "yes"})
            elif (df_reqd_cols[col_nm_col].str.lower() == k).any():
                if 'required' in v:
                    cols[k]['required'] = 'yes'
                else:
                    # print('Fixing PK required yes in ',file)
                    cols[k].update({'required': "yes"})
        else:
            if 'required' in v:
                cols[k]['required'] = 'yes'
            else:
                # print('Fixing PK required yes in ', file)
                cols[k].update({'required': "yes"})


def applyNumericPrecision(cols, file):
    for k, v in cols.items():
        if v['type'] == 'numeric':
            precision = v.get('precision', 'NA')
            if precision == 'NA':
                # print('Fixing number precision in ', file)
                cols[k].update({'precision': 22})


def addMissingPk(cols, file):
    tables = ['table_version_info']
    pk_def = {'primary_key': {'table_version_info_pk': ['table_name', 'version'], 'tablespace': 'rteidx'}}
    cols.update(pk_def)


def removePKIndex(cols, file):
    # This code removes explicit index mentioned in yaml for pk
    for k, v in cols.items():
        pk_ = v.get('primary_key', 'NA')
        if pk_ != 'NA':
            indx_ = v.get('index', 'NA')
            if indx_ != 'NA':
                v.pop('index')


if __name__ == "__main__":
    start_time = time.time()

    parser = argparse.ArgumentParser()
    parser.add_argument('--inDir', '-i', help="Path to folder containing input yaml file ", type=str)
    parser.add_argument('--outDir', '-o', help="Path to folder containing output yaml file ", type=str)
    parser.add_argument('--logFile', '-l', help="Log file ", type=str)
    parser.add_argument('--dataCheck', '-d', help="Check and modify data files", type=str)

    args = parser.parse_args()
    in_path = args.inDir
    out_path = args.outDir
    log_file = args.logFile
    data_check = args.dataCheck
    # print(pd.show_versions())
    logging.basicConfig(filename=log_file, filemode='a', level=logging.INFO,
                        format='%(asctime)s %(levelname)s: %(funcName)s:%(lineno)d %(message)s', \
                        datefmt='%Y-%m-%d %H:%M:%S')

    dump_flag = 'ora'
    # in_path is different for oracle dump and pg dump
    if str(Path('/gen')) in in_path:
        # Oracle version
        dump_flag = 'ora'
    else:
        # PG Version
        dump_flag = 'pg'

    if dump_flag == 'ora':
        # Oracle version
        in_dump_folder = in_path.split(str(Path('/gen')))[0]
    else:
        # PG Version
        in_dump_folder = in_path.split(str(Path('/csd')))[0]

    in_data_dump_folder = Path(f"{in_dump_folder}/data")
    in_data_dump_folder_tmp = Path(f"{in_data_dump_folder}/orig")
    if not os.path.exists(in_data_dump_folder_tmp):
        os.mkdir(in_data_dump_folder_tmp)

    if dump_flag == 'ora':
        # Oracle version
        app_tabcols_file = Path(f"{in_data_dump_folder}/oraexp_application_tab_cols.csv")
        cols = ['OWNER', 'TABLE_NAME', 'COLUMN_ID', 'COLUMN_NAME', 'NULLABLE', 'DATA_DEFAULT']
    else:
        app_tabcols_file = Path(f"{in_data_dump_folder}/platform.application_tab_cols.csv")
        cols = ['owner', 'table_name', 'column_id', 'column_name', 'nullable', 'data_default']

    pd.set_option('display.max_columns', None)
    # Postgres csv data file, \N is defined as null value
    dfAppTabCols = pd.read_csv(app_tabcols_file, header=0, na_values="\\N", usecols=cols, keep_default_na=False)
    fmt = "%a %b %d %H:%M:%S %Z %Y"

    infiles = glob.glob(in_path + '/*.yaml')
    keys = ["primary_key", "index"]
    fn = ""
    yaml = ruamel.yaml.YAML()
    yaml.default_flow_style = False
    yaml.explicit_start = True
    yaml.RoundTripDumper = True
    yaml.width = 2052
    yaml.indent(mapping=4, sequence=4, offset=2)
    for file in infiles:
        fn = os.path.basename(file)
        #print(f"Input file = {file}")
        if dump_flag == 'ora':
            schema = ''
            tabName = os.path.basename(file).split('.')[0].upper()
            datafilenm = f"oraexp_{tabName.lower()}.csv"
            tab_nm_col = 'TABLE_NAME'
            null_col = 'NULLABLE'
            col_nm_col = 'COLUMN_NAME'
            data_def_col = 'DATA_DEFAULT'
            tab_owner = 'OWNER'
        else:
            schema = ''
            tabName =  os.path.basename(file).split('.')[0].upper()
            datafilenm = ''
            tab_nm_col = 'table_name'
            null_col = 'nullable'
            col_nm_col = 'column_name'
            data_def_col = 'data_default'
            tab_owner = 'owner'
        # Required columns form *application_tab_cols csv
        df_reqd_cols = dfAppTabCols[(dfAppTabCols[tab_nm_col] == tabName) & (dfAppTabCols[null_col] == 'N')]
        with open(file, 'r') as fh:
            # Cleaning up any tmp files
            files = glob.glob(f"{in_data_dump_folder_tmp}/*")
            for f in files:
                os.remove(f)
            data = yaml.load(fh)
            for path, value in processKeys(keys, data):
                # ##print(path, '->', value)
                None
            if fn == 'table_version_info.yaml':
                pk_ = data['database_properties'].get('primary_key', 'NA')
                if pk_ == 'NA':
                    addMissingPk(data['database_properties'], fn)
            # Checking for single column pk
            # Modifying csd yaml files to match data in *application_tab_cols csv file within the dump.
            applyRequired(data['members'], True, fn, df_reqd_cols, in_data_dump_folder, in_data_dump_folder_tmp, dump_flag)
            pk_props = data['database_properties'].get('primary_key')
            # Checking for multi column pk
            if pk_props:
                # print('Processing ..', fn)
                for k, v in pk_props.items():
                    if k != 'tablespace':
                        # Filter columns that make multi-column pk
                        filtered_members = dict(filter(lambda val: val[0] in list(v), data['members'].items()))
                        applyRequired(filtered_members, False, fn, df_reqd_cols, in_data_dump_folder,
                                      in_data_dump_folder_tmp, dump_flag)
            applyNumericPrecision(data['members'], fn)
            removePKIndex(data['members'], fn)
            # Writing the modified yaml file
            with open(out_path + '/' + fn, 'w') as outfile:
                yaml.dump(data, outfile)

            if data_check.upper() == 'Y':
                if len(df_reqd_cols.index) > 0:
                    schema = df_reqd_cols[tab_owner].iloc[0].lower()
                if dump_flag == 'pg':
                    datafilenm = f"{schema}.{tabName.lower()}.csv"
                # Datafile for the corresponding yaml file
                # datafile = Path(f"{datafilenm}")
                if not tabName in skip_table:
                    datafile = Path(f"{in_data_dump_folder}/{datafilenm}")
                    if os.path.exists(datafile):
                        modifyDataFile(datafile, df_reqd_cols, data['members'],
                                   in_data_dump_folder_tmp, dump_flag,tabName,data_def_col)  # df_reqd_cols,cols

            #print(" Total duration = "+ str(time.time() - start_time) )

