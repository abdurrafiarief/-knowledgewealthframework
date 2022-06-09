

class QueryBuilder:
    '''
    Query builder class for building the queries needed

    Attributes:
    - prefix_string: prefix string for queries
    '''
    
    def __init__(self, prefixes):
        self.prefix_string = self.construct_prefix_string(prefixes)
        
    def construct_query_outgoing(self, filter_string, additional_filter_string, limit, distinct):
        '''
        Helper function to construct query string for outgoing properties
        Input:
        -filter_sting: string, class filters for the query
        -additional_filter_string: string, additional filters that may be needed
        -limit: int, entity max limit
        -distinct: boolean, true if for querying distinct properties
        Output:
        -query: string
        '''
        distinct_string = ""
        if distinct:
            distinct_string = "DISTINCT"
    
        query = self.prefix_string + '''
                SELECT ?s (COUNT(?p) AS ?pCount) {

                  {SELECT '''+ distinct_string +''' ?s ?p 
                  WHERE {
              ''' + filter_string + \
              '''
                    ?s ?p ?o . # get triples with the following constraints
                    '''+additional_filter_string+'''
                  }}

                } GROUP BY ?s LIMIT '''+ str(limit) +'''
              '''
        return query

    def construct_query_incoming(self, filter_string, additional_filter_string, limit, distinct):
        '''
        Helper function to construct query string for incoming properties
        Input:
        -filter_sting: string, class filters for the query
        -additional_filter_string: string, additional filters that may be needed
        -limit: int, entity max limit
        -distinct: boolean, true if for querying distinct properties
        Output:
        -query: string
        '''
        distinct_string = ""
        if distinct:
            distinct_string = "DISTINCT"
        
        query = self.prefix_string + '''
                SELECT ?s (COUNT(?i) AS ?iCount) {

                  {SELECT ''' + distinct_string +''' ?s ?i
                  WHERE {
              ''' + filter_string + \
              '''
                    ?o ?i ?s . 
                     '''+additional_filter_string+'''
                  }}

                } GROUP BY ?s LIMIT '''+ str(limit) +'''
              '''
        return query


    def construct_batch_query_outgoing(self, filter_string, values_list, additional_filter_string, distinct):
        '''
        Helper function to construct batch queries for outgoing properties
        Input:
        -filter_sting: string, class filters for the query
        -additional_filter_string: string, additional filters that may be needed
        -values_list: list of entity values, used for querying multiple entities at once
        -distinct: boolean, true if for querying distinct properties
        Output:
        -query: string
        '''
        values = ""
        for v in values_list:
            values += " <{}> ".format(v)
        
        distinct_string = ""
        if distinct:
            distinct_string = "DISTINCT"

        query = self.prefix_string + '''
                SELECT ?s (COUNT(?p) AS ?pCount) {

                  {SELECT  ''' + distinct_string +''' ?s ?p
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

    def construct_batch_query_incoming(self, filter_string, values_list, additional_filter_string, distinct):
        '''
        Helper function to construct batch queries for incoming properties
        Input:
        -filter_sting: string, class filters for the query
        -additional_filter_string: string, additional filters that may be needed
        -values_list: list of entity values, used for querying multiple entities at once
        -distinct: boolean, true if for querying distinct properties

        Output:
        -query: string
        '''
        values = ""
        for v in values_list:
            values += " <{}> ".format(v)
                
        distinct_string = ""
        if distinct:
            distinct_string = "DISTINCT"
            
        query = self.prefix_string + '''
                SELECT ?s (COUNT(?i) AS ?iCount) {

                  {SELECT ''' + distinct_string +''' ?s ?i
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

    def construct_sample_entities_query(self, filter_string, additional_filter_string, limit):
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
    
    def construct_get_all_classes_query(self, class_property='wdt:P31', class_identifer = "", class_additional_filter_string=[]):
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
    
    def construct_prefix_string(self, prefixes):
        #Helper function to create prefix string needed for queries

        prefix_string = ""
        for f in prefixes:
            prefix_string += ("PREFIX {} \n".format(f))

        return prefix_string

    def construct_filter_string(self, class_filters):
        #Helper function to create class filter string needed for queries

        filter_string = ""
        for f in class_filters:
            filter_string += ("?s {} . \n".format(f))

        return filter_string

    def construct_additional_filter_string(self, additional_filters): 
        #Helper function to create additional filters string needed for queries

        additional_filter_string = ""
        for f in additional_filters:
            additional_filter_string += "{} \n".format(f)

        return additional_filter_string