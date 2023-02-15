import os
import re
from pprint import pprint
import sys
import readline
import traceback
import shutil
import pickle
import pandas as panda 
sys.path.append('miniDB')

from miniDB.database import Database
from miniDB.table import Table

from miniDB.database import Database
from miniDB.table import Table

# art font is "big"
art = '''
             _         _  _____   ____  
            (_)       (_)|  __ \ |  _ \     
  _ __ ___   _  _ __   _ | |  | || |_) |
 | '_ ` _ \ | || '_ \ | || |  | ||  _ < 
 | | | | | || || | | || || |__| || |_) |
 |_| |_| |_||_||_| |_||_||_____/ |____/   2022                              
'''   


def search_between(s, first, last):
    '''
    Search in 's' for the substring that is between 'first' and 'last'
    '''
    print('SEARCHER')
    print(s)
    try:
        start = s.index( first ) + len( first )
        end = s.index( last, start )
    except:
        return
    return s[start:end].strip()

def in_paren(qsplit, ind):
    '''
    Split string on space and return whether the item in index 'ind' is inside a parentheses
    '''
    return qsplit[:ind].count('(')>qsplit[:ind].count(')')


def create_query_plan(query, keywords, action):
    '''
    Given a query, the set of keywords that we expect to pe p
    ent and the overall action, return the query plan for this query.
    This can and will be used recursively
    '''
    print('PRINTARO QUERY')
    
    print(query)
    print(action)

    
    

   
    dic = {val: None for val in keywords if val!=';'}

    ql = [val for val in query.split(' ') if val !='']

    kw_in_query = []
    kw_positions = []
    i=0
    while i<len(ql):
        if in_paren(ql, i): 
            i+=1
            continue
        if ql[i] in keywords:
            kw_in_query.append(ql[i])
            kw_positions.append(i)
        
        elif i!=len(ql)-1 and f'{ql[i]} {ql[i+1]}' in keywords:
            kw_in_query.append(f'{ql[i]} {ql[i+1]}')
            ql[i] = ql[i]+' '+ql[i+1]
            ql.pop(i+1)
            kw_positions.append(i)
        i+=1
    
    print('KEYWORD IN QUERY')
    print(*kw_in_query)
    print(action)
        


    for i in range(len(kw_in_query)-1):
        dic[kw_in_query[i]] = ' '.join(ql[kw_positions[i]+1:kw_positions[i+1]])
    
    if action == 'create view':
        print('ACTION = CREATE VIEW')
        dic['as'] = interpret(dic['as'])

    if action=='select':
        print('ACTION = SELECT')
        dic = evaluate_from_clause(dic)

        if dic['distinct'] is not None:
            dic['select'] = dic['distinct']
            dic['distinct'] = True

        if dic['order by'] is not None:
            dic['from'] = dic['from']
            if 'desc' in dic['order by']:
                dic['desc'] = True
            else:
                dic['desc'] = False
            dic['order by'] = dic['order by'].removesuffix(' asc').removesuffix(' desc')
            
        else:
            dic['desc'] = None

    if action=='create table':
        print('ACTION = CREATE TABLE')

        query_list = query.split()
        table_index = query_list.index("table")
        tab_name = query_list[table_index + 1]
        args = dic['create table'][dic['create table'].index('('):dic['create table'].index(')')+1]
        print('PROTO ARGLIST THELO TABLE')
        print(*args)
        dic['create table'] = dic['create table'].removesuffix(args).strip()
        arg_nopk = args.replace('primary key', '')[1:-1]
        arg_noUnique=args.replace('unique', '')[1:-1]
        arglist = [val.strip().split(' ') for val in arg_nopk.split(',')]
        print('ARGLIST')
        print(*arglist)
        print('ARgno pk ')
        print(arg_nopk)
        dic['column_names'] = ','.join([val[0] for val in arglist])
        dic['column_types'] = ','.join([val[1] for val in arglist])

        keyboy=0
        if 'primary key' in args:
            arglist = args[1:-1].split(' ')
            dic['primary key'] = arglist[arglist.index('primary')-2]
            keyboy=arglist[arglist.index('primary')-2]
            print('PRINTARO DIC PRIMAR KEY ')
            print(dic['primary key'])
        else:
            dic['primary key'] = None

        
        if 'unique' in arg_nopk:
            split_arg = arg_nopk.split()
            print('EXO UNIQUE STO ARG NO PK PRINT SPLIT ARG')
            print(split_arg)
            row1= split_arg[split_arg.index('unique') - 2]
            print('PRINTARO ROW ')
            print(row1)
            table1=tab_name
            data = {"tab_name": table1,"primary_key":keyboy, "unique_column": row1}
            #data=row1,'',keyboy

            if 'unique_table' in locals():
               
                dataFR=unique_table
                print('YPARXI ARXIO')
                '''
                with open("unique.pkl", "wb") as file:
                    pickle.dump(data, file)
                '''
                print('FTIAXNO PANDA')
                
            elif os.path.isfile('./unique_table.pkl'):
                dataFR=panda.read_pickle('./unique_table.pkl')  

            else:
                print('den YPARXI ARXIO')
                dataFR = panda.DataFrame(columns=['tab_name', 'primary_key', 'unique_column'])
                
                '''
                with open("unique.pkl", "rb") as f:
                    insiders=pickle.load(f)
                insiders.update
                '''

            dataFR=dataFR.append({'tab_name': table1, 'primary_key': keyboy, 'unique_column': row1},ignore_index=True)

            unique_table=dataFR
            dataFR.to_pickle('./unique_table.pkl')           




            
            #dic['uniques']=row1
            with open("unique_table.pkl", "rb") as file:
                content = pickle.load(file)
            print(content)

            
            

            
        

            
        
                
        '''
        for i in arglist:
            for j in i:
                if j == 'unique':
                    unique_column = i[0]
                    uniques.append(unique_column)
                    print('PRINTARO TO ONOMA TOU UNIQUE COLUMN')
                    print(*uniques)
        dic['unique'] = uniques if uniques else None
        '''






        
    
    if action=='import': 
        print('ACTION = IMPORT')
        dic = {'import table' if key=='import' else key: val for key, val in dic.items()}

    if action=='insert into':
        print('ACTION = INSERT INTO')
        if dic['values'][0] == '(' and dic['values'][-1] == ')':
            dic['values'] = dic['values'][1:-1]
        else:
            raise ValueError('Your parens are not right m8')
    
    if action=='unlock table':
        print('ACTION= UNLOCK TABLE')
        if dic['force'] is not None:
            dic['force'] = True
        else:
            dic['force'] = False

    return dic
def create_and_write_to_file(file_name, content):
    with open(file_name, "w") as file:
        file.write(content)
    print(f"File '{file_name}' created and written to successfully!")

#create_and_write_to_file("new_file.txt", "This is the content written to the file.")





def evaluate_from_clause(dic):
    '''
    Evaluate the part of the query (argument or subquery) that is supplied as the 'from' argument
    '''
    print('BIKA STO EVALUATE FROM CLAUSE')
    join_types = ['inner', 'left', 'right', 'full', 'sm', 'inl']
    from_split = dic['from'].split(' ')
    if from_split[0] == '(' and from_split[-1] == ')':
        subquery = ' '.join(from_split[1:-1])
        dic['from'] = interpret(subquery)

    join_idx = [i for i,word in enumerate(from_split) if word=='join' and not in_paren(from_split,i)]
    on_idx = [i for i,word in enumerate(from_split) if word=='on' and not in_paren(from_split,i)]
    if join_idx:
        join_idx = join_idx[0]
        on_idx = on_idx[0]
        join_dic = {}
        if from_split[join_idx-1] in join_types:
            join_dic['join'] = from_split[join_idx-1]
            join_dic['left'] = ' '.join(from_split[:join_idx-1])
        else:
            join_dic['join'] = 'inner'
            join_dic['left'] = ' '.join(from_split[:join_idx])
        join_dic['right'] = ' '.join(from_split[join_idx+1:on_idx])
        join_dic['on'] = ''.join(from_split[on_idx+1:])

        if join_dic['left'].startswith('(') and join_dic['left'].endswith(')'):
            join_dic['left'] = interpret(join_dic['left'][1:-1].strip())

        if join_dic['right'].startswith('(') and join_dic['right'].endswith(')'):
            join_dic['right'] = interpret(join_dic['right'][1:-1].strip())

        dic['from'] = join_dic
        
    return dic

def interpret(query):
    '''
    Interpret the query.
    '''
    print('INTO INTERPETER')
    print(query)

    similar1 = r"create index (\w+) on (\w+)\((\w+)\) using btree"
    similar_to= re.search(similar1,query)
    ok='ok'
    if similar_to:
        print('SIMILAR BABYYYY')
        index_name=similar_to.group(1)
        table_name=similar_to.group(2)
        table_column=similar_to.group(3)
        print(index_name,"  ",table_name,"  ",table_column)
        if os.path.isfile('./index_uniques.pkl'):
            dataFR5=panda.read_pickle('./index_uniques.pkl')
            dataFR5.loc[0]=table_name,table_column,index_name
            dataFR5.to_pickle('./index_uniques.pkl')

            dataFR5=panda.read_pickle('index_uniques.pkl')
            print('EGINE H DOULEIA')
            print(dataFR5)
        else:
            dataFR5=panda.DataFrame(columns=['table_name','table_column','index_name'])
            dataFR5.loc[0]=table_name,table_column,index_name
            dataFR5.to_pickle('./index_uniques.pkl')
            dataFR5=panda.read_pickle('index_uniques.pkl')
            print('EGINE H DOULEIA')
            print(dataFR5)

        
        


    kw_per_action = {'create table': ['create table'],
                     'drop table': ['drop table'],
                     'cast': ['cast', 'from', 'to'],
                     'import': ['import', 'from'],
                     'export': ['export', 'to'],
                     'insert into': ['insert into', 'values'],
                     'select': ['select', 'from', 'where', 'distinct', 'order by', 'limit'],
                     'lock table': ['lock table', 'mode'],
                     'unlock table': ['unlock table', 'force'],
                     'delete from': ['delete from', 'where'],
                     'update table': ['update table', 'set', 'where'],
                     'create index': ['create index', 'on', 'using'],
                     'drop index': ['drop index'],
                     'create view' : ['create view', 'as']
                     }

    if query[-1]!=';':
        query+=';'
    
    query = query.replace("(", " ( ").replace(")", " ) ").replace(";", " ;").strip()

    for kw in kw_per_action.keys():
        if query.startswith(kw):
            action = kw
            print('MESA APO INTERPRET PRINTARO ACTION')
            print(action)

    return create_query_plan(query, kw_per_action[action]+[';'], action)

def execute_dic(dic):
    '''
    Execute the given dictionary
    '''
    print('EIMAI STO EXECUTE DIC')
    for key in dic.keys():
        if isinstance(dic[key],dict):
            dic[key] = execute_dic(dic[key])
    
    action = list(dic.keys())[0].replace(' ','_')
    return getattr(db, action)(*dic.values())

def interpret_meta(command):
    """
    Interpret meta commands. These commands are used to handle DB stuff, something that can not be easily handled with mSQL given the current architecture.
    The available meta commands are:
    lsdb - list databases
    lstb - list tables
    cdb - change/create database
    rmdb - delete database
    """
    print('BENO INTERPRET METAS')
    action = command.split(' ')[0].removesuffix(';')

    db_name = db._name if search_between(command, action,';')=='' else search_between(command, action,';')

    verbose = True
    if action == 'cdb' and ' -noverb' in db_name:
        db_name = db_name.replace(' -noverb','')
        verbose = False

    def list_databases(db_name):
        [print(fold.removesuffix('_db')) for fold in os.listdir('dbdata')]
    
    def list_tables(db_name):
        [print(pklf.removesuffix('.pkl')) for pklf in os.listdir(f'dbdata/{db_name}_db') if pklf.endswith('.pkl')\
            and not pklf.startswith('meta')]

    def change_db(db_name):
        global db
        db = Database(db_name, load=True, verbose=verbose)
    
    def remove_db(db_name):
        shutil.rmtree(f'dbdata/{db_name}_db')

    commands_dict = {
        'lsdb': list_databases,
        'lstb': list_tables,
        'cdb': change_db,
        'rmdb': remove_db,
    }

    commands_dict[action](db_name)


if __name__ == "__main__":
    fname = os.getenv('SQL')
    dbname = os.getenv('DB')

    db = Database(dbname, load=True)
    
    print("BENO STO IF NAME MAIN KATI PAIZEI ")
    

    if fname is not None:
        for line in open(fname, 'r').read().splitlines():
            if line.startswith('--'): continue
            if line.startswith('explain'):
                dic = interpret(line.removeprefix('explain '))
                pprint(dic, sort_dicts=False)
            else :
                dic = interpret(line.lower())
                result = execute_dic(dic)
                if isinstance(result,Table):
                    result.show()
        

    from prompt_toolkit import PromptSession
    from prompt_toolkit.history import FileHistory
    from prompt_toolkit.auto_suggest import AutoSuggestFromHistory

    print(art)
    session = PromptSession(history=FileHistory('.inp_history'))
    while 1:
        try:
            line = session.prompt(f'({db._name})> ', auto_suggest=AutoSuggestFromHistory()).lower()
            if line[-1]!=';':
                line+=';'
        except (KeyboardInterrupt, EOFError):
            print('\nbye!')
            break
        try:
            if line=='exit':
                break
            if line.split(' ')[0].removesuffix(';') in ['lsdb', 'lstb', 'cdb', 'rmdb']:
                interpret_meta(line)
            elif line.startswith('explain'):
                dic = interpret(line.removeprefix('explain '))
                pprint(dic, sort_dicts=False)
            else:
                dic = interpret(line)
                result = execute_dic(dic)
                if isinstance(result,Table):
                    result.show()
        except Exception:
            print(traceback.format_exc())