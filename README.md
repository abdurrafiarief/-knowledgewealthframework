# Knowledgewealthframework

This is a framework for analyzing knowledge wealth within Knowledge Graphs. This framework constructs queries according to specification, queries a SPARQL endpoint and provides an object to help with analyzing knowledge wealth. 

### Import the package
```python
#Import the WealthKG object from the python file
from WealthKG import wealthKG
```

### Connecting to a SPARQL Endpoint and querying

```python
# First intialize the framework with a SPARQL endpoint and any needed prefixes. It will return an object to help with querying
url = 'https://query.wikidata.org/sparql'
prefixes = ["wd: <http://www.wikidata.org/entity/>",
            "wdt: <http://www.wikidata.org/prop/direct/>"]
wealthKG = wealthKG.WealthKG(url, prefixes)

# Define the filters needed. The query uses a "?s <p> <o>" format so the filter used for the class will have
# to be a "<p> <o>" format.
class_filters = ['wdt:P31 wd:Q5', #is instance (P31) of human (Q5)
                 'wdt:P106 wd:Q82594', #has occupation (P106) of computer scientist (Q82594)
                ] 

# Define any aditional filters that may be need for the query. The format is flexible as long as it follows
# the SPARQL format.
add_filters = ['FILTER(CONTAINS(STR(?s),"wikidata.org/entity/Q"))',
               'FILTER(CONTAINS(STR(?p),"prop/direct/")) ',
               'FILTER(CONTAINS(STR(?i),"prop/direct/"))']

# Define if we want to just count distinct properties (distinct = True) or count all properties (distinct = False)
distinct = True

# Define the max amount of entities to query. If the class has less then the specified limit it will query all
# the entities
limit = 20000
cs_wealth = wealthKG.single_class_query(class_filters, add_filters, distinct, limit)
```

### Analyzing the object
```python 
# To access the dataframe, we can call the objects properties
cs_wealth.dataframe
```
![cs_wealth dataframe] (https://github.com/abdurrafiarief/knowledgewealthframework/blob/dd63475a378aee3fe3c9cbf77e4ae68eea713454/images/Wikidata%20Computer%20Scientist%20Dataframe.png)

```python
# We can use this function to create a histogram for a certain part or show the whole thing
# x-axis is the number of properties, y-axis is the number of entities with x amount of properties
# this histogram is showing the distribution of entities based on the number of properties the entities posses
cs_wealth.get_histogram(part='all')
```
![cs_wealth histogram for all types of properties](https://github.com/abdurrafiarief/knowledgewealthframework/blob/main/images/Wikidata%20Computer%20Scientist%20Histogram%20for%20Outgoing%20Properties.png)

```python
# It is possible to to see a histogram of a certain part
cs_wealth.get_histogram(part='pCount')
```
![cs_wealth histogram for outgoing properties](https://github.com/abdurrafiarief/knowledgewealthframework/blob/main/images/Wikidata%20Computer%20Scientist%20Histogram%20for%20Outgoing%20Properties.png)

```python
# This function can be used for geting a pareto chart for the class in a specified column
cs_wealth.get_pareto_chart('pCount')
```
![cs_wealth pareto chart](https://github.com/abdurrafiarief/knowledgewealthframework/blob/main/images/Wikidata%20Computer%20Scientist%20Pareto%20Chart.png)

```python
# This function returns a statistical summary for a particular column
cs_wealth.get_summary(part='pCount')
```
Output:
```
Q1 = 13.0
Q2/median = 19.0
Q3 = 30.0
Min =  2
Max =  363
mode = 11
mean = 24.4649926686217
kurtosis = 23.725308786005144
skewness = 3.311531452195597
..........
```

### Querying multiple Classes
```python
# As wealthKG was already initialized we don't need to initialize it again.
# Here is the demonstration for the multi class query. Before that here is an overview of the parameters

class_identifier = "" #The class usually used to denote if something is a class
class_property = "wdt:P31" #The property usually used to denote if an entity is an instance of a class

# Filters for the class. This example wants to get the classes that are a subclass(P279) of human(Q5)
class_add_filters = ['?class wdt:P279 wd:Q5 .']

# Filters for the properties to be queried
add_filters = ['FILTER(CONTAINS(STR(?s),"wikidata.org/entity/Q"))',
               'FILTER(CONTAINS(STR(?p),"prop/direct/")) ', 
               'FILTER(CONTAINS(STR(?i),"prop/direct/")) '] 
distinct = False #False if want to count every property statement

# limit for amount of classes to query. 0 if no limit (or query all the classes). Set 10 for demonstration purposes
class_limit = 10

# limit for amount of entities to query
limit = 10000

# This function is what is used for the query. As we are querying multiple classes it might take a while
# It returns an object that stores the dataframe of each class
human_subclasses = wealthKG.multiclass_query(class_property, class_identifier, 
                                             class_add_filters,  add_filters, 
                                             class_limit=class_limit, distinct=distinct, limit=limit)

```

### Analyzing multi class object

```python
#If want to access each dataframe we can call class_dict keys and use a key access the desired class
human_subclasses.class_dict.keys()
```
Output:

dict_keys(['Q7569', 'Q22947', 'Q26513', 'Q56054675', 'Q56054676', 'Q64520857', 'Q65720209', 'Q75855169', 'Q77020806', 'Q84048850'])

```python
human_subclasses.class_dict['Q7569']
```
![Q7569 Datafrane](https://github.com/abdurrafiarief/knowledgewealthframework/blob/main/images/Wikidata%20Human%20Subclass%20Dataframe%20Example.png)

```python
# If want to see all histograms for the class than use this function
# x-axis is the number of properties, y-axis is the number of entities with x amount of properties
# this histogram is showing the distribution of entities based on the number of properties the entities posses
human_subclasses.get_all_histogram(part='pCount', title_text="10 Subclasses of Human")
```
![human_subclass histograms] (https://github.com/abdurrafiarief/knowledgewealthframework/blob/main/images/Wikidata%20Human%20Subclass%20Histogram.png)

```python
# We can also some analysis for the entities
human_subclasses.get_average_entities()
```
Output:

23.4


```python
human_subclasses.get_entity_count_histogram()
```
![human_subclass entity count](https://github.com/abdurrafiarief/knowledgewealthframework/blob/main/images/Wikidata%20Human%20Subclass%20Entity%20Histogram.png)
```python
# It is also possible to save the files into csv-s stored in a folder
human_subclasses.save_to_csv_folder("human_subclasses")
```

Output:

Saved to human_subclasses

```python
# And with the WealthKG object we can read from directories.
# Just input the folder directory into the function.
#IMPORTANT NOTE: for windows directories, add an r before the string. Example: r"user\sample_folder\"

human_subclasses = wealthKG.read_csv_folder(r"human_subclasses\")
human_subclasses.class_list
```
![human_subclass class_list](https://github.com/abdurrafiarief/knowledgewealthframework/blob/main/images/Wikidata%20Human%20Subclass%20Class%20List.png)

