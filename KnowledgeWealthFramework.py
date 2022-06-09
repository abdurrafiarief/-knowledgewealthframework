#Import the libraries needed
import pandas as pd
import math
import time



class WealthKG:
    '''
    Instantiate WealthKG class.

    Attributes:
    - sparql_endpoint: string. Endpoint for SPARQL server
    - prefixes: list of string. prefixes that might be needed for SPARQL endpoint.
    '''

    def __init__(self, sparql_endpoint, prefixes = []):
        if validators.url(sparql_endpoint):
            self.sparql_endpoint = sparql_endpoint
        else:
            raise Exception("URL not valid")
        self.prefix_string = self.__construct_prefix_string(prefixes)
    
  
    def single_class_query(self, class_filters, additional_filters=[], bag=True, limit=10000):
        '''
        Function for querying entities within a class

        Inputs:
        - class_filters: string list. List with filters needed for the class
        - additional_filters: string list. List with any additional special filters. Default empty.
        - bag: boolean. True for bag queries, false for set queries. Default True.
        - limit: int. Maximum ammount of entities to be queried. Default 10 thousand.
        
        Output:
        -WealthKGSingleClassObject: Object for the results of the query
        '''

        filter_string = self.__construct_filter_string(class_filters)
        out_filters = [x for x in additional_filters if "?p" in x] + [x for x in additional_filters if "?s" in x]
        in_filters = [x for x in additional_filters if "?i" in x] + [x for x in additional_filters if "?s" in x]
        additional_filter_string_out = self.__construct_additional_filter_string(out_filters)
        additional_filter_string_in = self.__construct_additional_filter_string(in_filters)
            
            if limit <= 10000 or "dbpedia" in self.sparql_endpoint:
                if bag:
                query_string_outgoing = self.__construct_query_bag_outgoing(filter_string, additional_filter_string_out, limit)
                query_string_incoming = self.__construct_query_bag_incoming(filter_string, additional_filter_string_in, limit)

                df = self.__construct_df_outgoing_incoming(query_string_outgoing, query_string_incoming)

                return WealthKGSingleClassObject(df, bag, filter_string, len(df))

                else:
                query_string_outgoing = self.__construct_query_set_outgoing(filter_string, additional_filter_string_out, limit)
                query_string_incoming = self.__construct_query_set_incoming(filter_string, additional_filter_string_in, limit)

                df = self.__construct_df_outgoing_incoming(query_string_outgoing, query_string_incoming)

                return WealthKGSingleClassObject(df, bag, filter_string, len(df))
            
            #If more than 10000 construct the query in a different way
            else:
                entity_additional_filter = [x for x in additional_filters if "?s" in x]
                entity_additional_filter_string = self.__construct_additional_filter_string(entity_additional_filter)
                sample_query = self.__construct_sample_entities_query(filter_string, entity_additional_filter_string, limit)
                entities_list = self.__get_entities_list(sample_query)

                if bag:
                    df = self.__construct_batch_bag_df(filter_string, entities_list, additional_filters)
                    return WealthKGSingleClassObject(df, bag, filter_string, len(df))

                else:
                    df = self.__construct_batch_set_df(filter_string, entities_list, additional_filters)
                    return WealthKGSingleClassObject(df, bag, filter_string, len(df))
  
    def __construct_query_bag_outgoing(self, filter_string, additional_filter_string, limit):
        '''
        Helper function to construct query string for outgoing properties
        Input:
        -filter_sting: string, class filters for the query
        -additional_filter_string: string, additional filters that may be needed
        -limit: int, entity max limit

        Output:
        -query: string
        '''


        query = self.prefix_string + '''
                SELECT ?s (COUNT(?p) AS ?pCount) {

                  {SELECT ?s ?p 
                  WHERE {
              ''' + filter_string + \
              '''
                    ?s ?p ?o . # get triples with the following constraints
                    '''+additional_filter_string+'''
                  }}

                } GROUP BY ?s LIMIT '''+ str(limit) +'''
              '''
        return query
  
    def __construct_query_set_outgoing(self, filter_string, additional_filter_string, limit):
        '''
        Helper function to construct query string for distinct outgoing properties
        Input:
        -filter_sting: string, class filters for the query
        -additional_filter_string: string, additional filters that may be needed
        -limit: int, entity max limit

        Output:
        -query: string
        '''

        query = self.prefix_string + '''
                SELECT ?s (COUNT(?p) AS ?pCount) {

                  {SELECT DISTINCT ?s ?p
                  WHERE {
              ''' + filter_string + \
              '''
                    ?s ?p ?o . # get 
                     '''+additional_filter_string+'''
                  }}

                } GROUP BY ?s LIMIT '''+ str(limit) +'''
              '''
        return query

    def __construct_query_bag_incoming(self, filter_string, additional_filter_string, limit):
        '''
        Helper function to construct query string for incoming properties
        Input:
        -filter_sting: string, class filters for the query
        -additional_filter_string: string, additional filters that may be needed
        -limit: int, entity max limit

        Output:
        -query: string
        '''
        query = self.prefix_string + '''
                SELECT ?s (COUNT(?i) AS ?iCount) {

                  {SELECT  ?s ?i
                  WHERE {
              ''' + filter_string + \
              '''
                    ?o ?i ?s . 
                     '''+additional_filter_string+'''
                  }}

                } GROUP BY ?s LIMIT '''+ str(limit) +'''
              '''
        return query

    def __construct_query_set_incoming(self, filter_string, additional_filter_string, limit):
        '''
        Helper function to construct query string for distinct incoming properties
        Input:
        -filter_sting: string, class filters for the query
        -additional_filter_string: string, additional filters that may be needed
        -limit: int, entity max limit

        Output:
        -query: string
        '''
        
        query = self.prefix_string + '''
                SELECT ?s (COUNT(?i) AS ?iCount) {

                  {SELECT DISTINCT ?s ?i
                  WHERE {
              ''' + filter_string + \
              '''
                    ?o ?i ?s . 
                     '''+additional_filter_string+'''
                  }}

                } GROUP BY ?s LIMIT '''+ str(limit) +'''
              '''
        return query

    def __construct_batch_query_bag_outgoing(self, filter_string, values_list, additional_filter_string):
        '''
        Helper function to construct batch queries for outgoing properties
        Input:
        -filter_sting: string, class filters for the query
        -additional_filter_string: string, additional filters that may be needed
        -values_list: list of entity values, used for querying multiple entities at once

        Output:
        -query: string
        '''
        values = ""
        for v in values_list:
            values += " <{}> ".format(v)

        query = self.prefix_string + '''
                SELECT ?s (COUNT(?p) AS ?pCount) {

                  {SELECT  ?s ?p
                  WHERE {
                  VALUES ?s {'''+values+'''}
              ''' + filter_string + \
              '''
                    ?s ?p ?o . # get 
                    '''+additional_filter_string+'''
                  }}

                } GROUP BY ?s 
              '''
        return query

    def __construct_batch_query_bag_incoming(self, filter_string, values_list, additional_filter_string):
        '''
        Helper function to construct batch queries for incoming properties
        Input:
        -filter_sting: string, class filters for the query
        -additional_filter_string: string, additional filters that may be needed
        -values_list: list of entity values, used for querying multiple entities at once

        Output:
        -query: string
        '''
        values = ""
        for v in values_list:
            values += " <{}> ".format(v)

        query = self.prefix_string + '''
                SELECT ?s (COUNT(?i) AS ?iCount) {

                  {SELECT  ?s ?i
                  WHERE {
                  VALUES ?s {'''+values+'''}
              ''' + filter_string + \
              '''
                    ?o ?i ?s . # get 
                    '''+additional_filter_string+'''
                  }}

                } GROUP BY ?s 
              '''
        return query

    def __construct_batch_query_set_outgoing(self, filter_string, values_list, additional_filter_string):
        '''
        Helper function to construct batch queries for distinct outgoing properties
        Input:
        -filter_sting: string, class filters for the query
        -additional_filter_string: string, additional filters that may be needed
        -values_list: list of entity values, used for querying multiple entities at once

        Output:
        -query: string
        '''

        values = ""
        for v in values_list:
            values += " <{}> ".format(v)

        query = self.prefix_string + '''
                SELECT ?s (COUNT(?p) AS ?pCount) {

                  {SELECT DISTINCT ?s ?p
                  WHERE {
                  VALUES ?s {'''+values+'''}
              ''' + filter_string + \
              '''
                    ?s ?p ?o . # get 
                    '''+additional_filter_string+'''
                  }}

                } GROUP BY ?s 
              '''
        return query

    def __construct_batch_query_set_incoming(self, filter_string, values_list, additional_filter_string):
        '''
        Helper function to construct batch queries for distinct incoming properties
        Input:
        -filter_sting: string, class filters for the query
        -additional_filter_string: string, additional filters that may be needed
        -values_list: list of entity values, used for querying multiple entities at once

        Output:
        -query: string
        '''

        values = ""
        for v in values_list:
            values += " <{}> ".format(v)

        query = self.prefix_string + '''
                SELECT ?s (COUNT(?i) AS ?iCount) {

                  {SELECT DISTINCT  ?s ?i
                  WHERE {
                  VALUES ?s {'''+values+'''}
              ''' + filter_string + \
              '''
                    ?o ?i ?s . # get 
                    '''+additional_filter_string+'''
                  }}

                } GROUP BY ?s 
              '''
        return query
  
    def __construct_df_outgoing_incoming(self, query_out, query_in):
        '''
        Helper function to call endpoint and create df from results
        Input:
        -query_out: string, query string for outgoing properties
        -query_in: string, query string for incoming properties
        Output:
        -result_df: pandas dataframe, dataframe for query result ordered from total properties
        '''
        url = self.sparql_endpoint

        if "dbpedia" not in url:
            out_r = requests.post(url, data = {'format': 'json', 'query': query_out})
            outdata = out_r.json()
            outdf = pd.DataFrame.from_dict(outdata['results']['bindings'])

            try:
                outdf['s'] = outdf['s'].apply(self.__getValue)
                outdf['pCount'] = outdf['pCount'].apply(self.__getValue)
                outdf = outdf.astype({"pCount": int})
            except:
                outdf['s'] = []
                outdf['pCount'] = []

            in_r = requests.post(url, data = {'format': 'json', 'query': query_in})
            indata = in_r.json()
            indf = pd.DataFrame.from_dict(indata['results']['bindings'])

            try:
                indf['s'] = indf['s'].apply(self.__getValue)
                indf['iCount'] = indf['iCount'].apply(self.__getValue)
                indf = indf.astype({"iCount": int})
            except:
                indf['s'] = []
                indf['iCount'] = []

        else:
            out_r = requests.get(url, params = {'format': 'json', 'query': query_out})
            outdata = out_r.json()
            outdf = pd.DataFrame.from_dict(outdata['results']['bindings'])

            try:
                outdf['s'] = outdf['s'].apply(self.__getValue)
                outdf['pCount'] = outdf['pCount'].apply(self.__getValue)
                outdf = outdf.astype({"pCount": int})
            except:
                outdf['s'] = []
                outdf['pCount'] = []

            in_r = requests.get(url, params = {'format': 'json', 'query': query_in})
            indata = in_r.json()
            indf = pd.DataFrame.from_dict(indata['results']['bindings'])

            try:
                indf['s'] = indf['s'].apply(self.__getValue)
                indf['iCount'] = indf['iCount'].apply(self.__getValue)
                indf = indf.astype({"iCount": int})
            except:
                indf['s'] = []
                indf['iCount'] = []


        resultdf = pd.merge(
                    outdf,
                    indf,
                    how="left",
                    on="s",
                    left_index=False,
                    right_index=False,
                    suffixes=("_x", "_y"),
                    copy=True,
                )
        resultdf.fillna(0, inplace=True)

        resultdf["totalCount"] = resultdf["pCount"] + resultdf["iCount"]

        return resultdf.sort_values(by='totalCount', ascending=False).reset_index(drop=True)

    def __construct_batch_bag_df(self, filter_string, values_list, additional_filters):
        '''
        Helper function to create dataframe from sample query
        Input:
        -filter_sting: string, class filters for the query
        -additional_filter_string: string, additional filters that may be needed
        -values_list: list of entity values, used for querying multiple entities at once

        Output:
        -df: pandas dataframe, dataframe for query results
        '''
        upperRange = len(values_list)
        entities = []
        pCount = []
        iCount = []
        totalCount = []
        url = self.sparql_endpoint

        out_filters = [x for x in additional_filters if "?p" in x] + [x for x in additional_filters if "?s" in x]
        in_filters = [x for x in additional_filters if "?i" in x] + [x for x in additional_filters if "?s" in x]
        additional_filter_string_out = self.__construct_additional_filter_string(out_filters)
        additional_filter_string_in = self.__construct_additional_filter_string(in_filters)

        if len(values_list) == 0:
            df_dict = {"entity":entities, "pCount":pCount, "iCount":iCount, "totalCount": totalCount}
            df = pd.DataFrame(df_dict)
            return df

        for i in tqdm(range(0,upperRange,10000)):
            while True:
                try:
                    lower = i
                    upper = i+10000
                    if upper >= upperRange:
                        upper = upperRange-1

                    query_out = self.__construct_batch_query_bag_outgoing(filter_string, values_list[lower:upper], additional_filter_string_out)

                    query_in = self.__construct_batch_query_bag_incoming(filter_string, values_list[lower:upper], additional_filter_string_in)

                    df = self.__construct_df_outgoing_incoming(query_out, query_in)

                    entities += list(df['s'])
                    pCount += list(df['pCount'])
                    iCount += list(df['iCount'])
                    totalCount += list(df['totalCount'])

                except:
                    time.sleep(2)
                    continue
                break 

        df_dict = {"entity":entities, "pCount":pCount, "iCount":iCount, "totalCount":totalCount}
        df = pd.DataFrame(df_dict)
        df.drop_duplicates(keep='first', inplace=True)
        return df.sort_values(by='totalCount', ascending=False).reset_index(drop=True)


    def __construct_batch_set_df(self, filter_string, values_list, additional_filters):
        '''
        Helper function to create dataframe from sample query for distinct properties
        Input:
        -filter_sting: string, class filters for the query
        -additional_filter_string: string, additional filters that may be needed
        -values_list: list of entity values, used for querying multiple entities at once

        Output:
        -df: pandas dataframe, dataframe for query results
        '''
        
        upperRange = len(values_list)
        entities = []
        pCount = []
        iCount = []
        totalCount = []
        url = self.sparql_endpoint

        out_filters = [x for x in additional_filters if "?p" in x] + [x for x in additional_filters if "?s" in x]
        in_filters = [x for x in additional_filters if "?i" in x] + [x for x in additional_filters if "?s" in x]
        additional_filter_string_out = self.__construct_additional_filter_string(out_filters)
        additional_filter_string_in = self.__construct_additional_filter_string(in_filters)

        if len(values_list) == 0:
            df_dict = {"entity":entities, "pCount":pCount, "iCount":iCount, "totalCount": totalCount}
            df = pd.DataFrame(df_dict)
            return df

        for i in tqdm(range(0,upperRange,10000)):
            while True:
                try:
                    lower = i
                    upper = i+10000
                    if upper >= upperRange:
                        upper = upperRange-1

                    query_out = self.__construct_batch_query_set_outgoing(filter_string, values_list[lower:upper], additional_filter_string_out)

                    query_in = self.__construct_batch_query_set_incoming(filter_string, values_list[lower:upper], additional_filter_string_in)

                    df = self.__construct_df_outgoing_incoming(query_out, query_in)

                    entities += list(df['s'])
                    pCount += list(df['pCount'])
                    iCount += list(df['iCount'])
                    totalCount += list(df['totalCount'])

                except:
                    time.sleep(2)
                    continue
                break 

        df_dict = {"entity":entities, "pCount":pCount, "iCount":iCount, "totalCount":totalCount}
        df = pd.DataFrame(df_dict)
        df.drop_duplicates(keep='first', inplace=True)
        return df.sort_values(by='totalCount', ascending=False).reset_index(drop=True)
  

    def __construct_sample_entities_query(self, filter_string, additional_filter_string, limit):
        '''
        Helper function to create query for getting entities
        Input:
        -filter_sting: string, class filters for the query
        -additional_filter_string: string, additional filters that may be needed
        -limit: int, max amount of entities per class to query

        Output:
        -query: string, query string
        '''
        query = self.prefix_string + '''
                SELECT ?s WHERE {         
              ''' + filter_string + ''' ''' \
                  +additional_filter_string+'''
                } LIMIT '''+str(limit)+'''
              '''

        return query

    def __get_entities_list(self, query):
        '''
        Helper function to get the entity list from SPARQL endpoint
        Input:
        -query: string, query string

        Output:
        -list: list, list of entities
        '''
        
        url = self.sparql_endpoint
        r = requests.get(url, params = {'format': 'json', 'query': query})
        data = r.json()
        
        try:
            df = pd.DataFrame.from_dict(data['results']['bindings'])
            df['s'] = df['s'].apply(self.__getValue)
        except:
            return []

        return list(df['s'])
  
    def multiclass_query(self, class_property, class_identifier, class_additional_filters, additional_filters, bag=True, limit=10000):
        '''
        This function is for querying multiple class in a knowledge graph
        Input:
        -class_property: string, property for "is instance" or equivalent
        -class_identifer: string, URI for class if "class" is defined in the knowledge graph ontology
        -class_additional_filters: string, filters for querying class list
        -additional_filters: string, additional filter for querying each class
        -bag: boolean. True for bag queries, false for set queries. Default True.
        -limit: int. Maximum ammount of entities to be queried. Default 10 thousand.
        
        Output:
        -WealthKGMultiClassObject: Object for the results of the query
        '''
        class_add_string = self.__construct_additional_filter_string(class_additional_filters)
        class_query = self.__construct_get_all_classes_query(class_property, class_identifier, class_add_string)

        class_df = self.__construct_all_classes_df(class_query)


        if bag:
            result_dict = self.__get_all_bag_df(class_property=class_property, class_list = list(class_df["class"]), additional_filters=additional_filters, limit=limit)
            return WealthKGMultiClassObject(class_df, result_dict)
        else:
            result_dict = self.__get_all_set_df(class_property=class_property, class_list = list(class_df["class"]), additional_filters=additional_filters, limit=limit)
            return WealthKGMultiClassObject(class_df, result_dict)
     
  
    def __construct_get_all_classes_query(self, class_property='wdt:P31', class_identifer = "", class_additional_filter_string=[]):
        # 
        #Input: arr_prefixes (Array of prefixes needed for the KG), is_instance_property (property that indicates a class),
        #      min_count (to filter classes with a certain amount of entitites)
        #Output: query string for class
        '''
        This is a helper function is for getting all classes with in a KG
        Input:
        -class_property: string, property for "is instance" or equivalent
        -class_identifer: string, URI for class if "class" is defined in the knowledge graph ontology
        -class_additional_filters: string, filters for querying class list
            
        Output:
        -query: string, query string
        '''


        query = self.prefix_string + '''
            SELECT distinct ?class 
            WHERE {
            ?entity '''+class_property+''' ?class .
            '''+class_additional_filter_string+'''
            }'''

        if class_identifer != "":
            query = self.prefix_string + '''
                SELECT distinct ?class 
                WHERE {
                ?class '''+class_property+' '+class_identifer+'''  .
                '''+class_additional_filter_string+'''
                } '''

        return query
  
  
    def __construct_all_classes_df(self,  query):
        '''
        This function is for constructing a df for all classes in a kg
        Input:
        -query:string, query string
        Output: 
        -df: pandas dataframe
        '''
        url = self.sparql_endpoint
        r = requests.get(url, params = {'format': 'json', 'query': query})
        data = r.json()

        df = pd.DataFrame.from_dict(data['results']['bindings'])

        df['class'] = df['class'].apply(self.__getValue)

        return df


    def __get_all_bag_df(self, class_list, class_property, additional_filters, limit):
        '''
        This is a function to get entities of multiple classes from a KG
        Input:
        -query:string, query string
        -class_list: list, list of entities
        -class_property: string, property for "is instance" or equivalent
        -additional_filters: string, additional filter for querying each class
        -limit: int, max limit of entities
        Output: 
        -dict: dictionary with class identifier as the key and pandas dataframe of class as value
        '''

        df_dict = {}

        out_filters = [x for x in additional_filters if "?p" in x] + [x for x in additional_filters if "?s" in x]
        in_filters = [x for x in additional_filters if "?i" in x] + [x for x in additional_filters if "?s" in x]
        additional_filter_string_out = self.__construct_additional_filter_string(out_filters)
        additional_filter_string_in = self.__construct_additional_filter_string(in_filters)

        for class_uri in tqdm(class_list):
            while True:
                try:
                    key = class_uri
                    filter_string = "?s {} <{}> .".format(class_property, class_uri)
                    query_out = self.__construct_query_bag_outgoing(filter_string=filter_string, additional_filter_string=additional_filter_string_out, limit=limit)
                    query_in = self.__construct_query_bag_incoming(filter_string=filter_string, additional_filter_string=additional_filter_string_in, limit=limit)

                    class_df = self.__construct_df_outgoing_incoming(query_out, query_in)

                    df_dict[key] = class_df

                except:
                    time.sleep(0.1)
                    continue
                break

        return df_dict

    def __get_all_set_df(self, class_list, class_property, additional_filters, limit):
        '''
        This is a function to get entities of multiple classes from a KG with distinct properties
        Input:
        -query:string, query string
        -class_list: list, list of entities
        -class_property: string, property for "is instance" or equivalent
        -additional_filters: string, additional filter for querying each class
        -limit: int, max limit of entities
        Output: 
        -dict: dictionary with class identifier as the key and pandas dataframe of class as value
        '''

        df_dict = {}

        out_filters = [x for x in additional_filters if "?p" in x] + [x for x in additional_filters if "?s" in x]
        in_filters = [x for x in additional_filters if "?i" in x] + [x for x in additional_filters if "?s" in x]
        additional_filter_string_out = self.__construct_additional_filter_string(out_filters)
        additional_filter_string_in = self.__construct_additional_filter_string(in_filters)

        for class_uri in tqdm(class_list):
            while True:
                try:
                    key = class_uri
                    filter_string = "?s {} <{}> .".format(class_property, class_uri)
                    query_out = self.__construct_query_set_outgoing(filter_string=filter_string, additional_filter_string=additional_filter_string_out, limit=limit)
                    query_in = self.__construct_query_set_incoming(filter_string=filter_string, additional_filter_string=additional_filter_string_in, limit=limit)
                    class_df = self.__construct_df_outgoing_incoming(query_out, query_in)

                    df_dict[key] = class_df

                except:
                    time.sleep(0.1)
                    continue
                break

        return df_dict

    def __construct_prefix_string(self, prefixes):
        #Helper function to create prefix string needed for queries

        prefix_string = ""
        for f in prefixes:
            prefix_string += ("PREFIX {} \n".format(f))

        return prefix_string

    def __construct_filter_string(self, class_filters):
        #Helper function to create class filter string needed for queries

        filter_string = ""
        for f in class_filters:
            filter_string += ("?s {} . \n".format(f))

        return filter_string

    def __construct_additional_filter_string(self, additional_filters): 
        #Helper function to create additional filters string needed for queries

        additional_filter_string = ""
        for f in additional_filters:
            additional_filter_string += "{} \n".format(f)

        return additional_filter_string

    def __getValue(self, x):
        #Helper function to getvalue from json
        return x['value']
  
    def read_csv_folder(location):
        '''
        Function for reading folders filled with csv for classes
        input:
        -location: string, folder location
        output:
        -WealthKGMultiClassObject: object filled with dataframe for each class with at least one entity
        '''
        file_list = glob.glob("{}*.csv".format(location))
        result_dict = {}
        class_list = []
        for file_name in tqdm(file_list):
            df = pd.read_csv(file_name)
            df.fillna(0, inplace=True)
            if len(df) > 0:
                key = file_name.split('/')[-1][:-4]
                class_list.append(key)
                result_dict[key] = df
        result_object = WealthKGMultiClassObject(result_dict, class_list)
        return result_object