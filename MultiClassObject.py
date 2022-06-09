#Import the libraries needed
import pandas as pd
import numpy as np
import plotly.express as px
import matplotlib.pyplot as plt
import math
import kaleido
import plotly.graph_objects as go
import time
import glob
import os
from plotly.subplots import make_subplots
from scipy.stats import ks_2samp, norm, wasserstein_distance
from matplotlib.ticker import PercentFormatter
from tqdm import tqdm



class WealthKGMultiClassObject:
    '''
    Wealth KG Object for all classes within a knowledge graph

    Attributes:
    - class_dict: dictionary of each class' dataframe
    - bag: Boolean. True if bag was used for queries, false if set was used for queries.
    - class_count: int. Represents number of entities in analysis.
    - class_list: list. Represents each class in a list
    '''
    def __init__(self, class_dict, class_list):
        self.class_dict = class_dict
        self.class_list = class_list
  
    def show_histogram_all(self, title_text, part):
        '''
        This function creates a subplot filled with each class' histogram
        Input:
        -title_text: string, for setting the title of the plot
        -part: part of the dataframe to be mapped to histogram ("pCount", "iCount")
        
        output:
        -plotly figure for histogram
        '''
        key_list = list(self.class_dict.keys())
        key_list.sort()

        rows = math.ceil(len(key_list)/5)
        height = 2*120*rows

        subplot_titles = []
        for i in range(len(key_list)):
            subplot_titles.append("title")

        fig = make_subplots(
        rows=rows, cols=5, subplot_titles=subplot_titles
        )
        
        row = 1
        col = 1
        index = 0
        for key in key_list:
            fig.add_trace(go.Histogram(x=self.class_dict[key][part]), row=row, col=col)
            fig.layout.annotations[index].update(text=key)
            index += 1
            col += 1
            if col > 5:
                col = 1
                row += 1

        fig.update_layout(height=height, width=1200, title_text=title_text, title_x=0.5, showlegend=False)
        return fig

    def save_to_csv_folder(self, location):
        '''
        This function is for saving the dictionary of dataframes to csvs within a folder
        Input:
        -location: string, location of folder to save to. Can be preexisting
        
        '''
        
        outdir = location
        if not os.path.exists(outdir):
            os.mkdir(outdir)

        for key in self.class_dict.keys():
            outname = '{}.csv'.format(key.split('/')[-1])
            fullname = os.path.join(outdir, outname)    
            self.class_dict[key].to_csv(fullname)
        print("Saved to {}".format(location))

  
    def get_average_skewness(self, part):
        '''
        The function returns the average skewness of all the classes
        Input:
        -part: string, the column for which to calculate skewness (pCount or iCount)
        Output:
        -skewness: float
        '''
        skewness_arr = []
        key_list = list(self.class_dict.keys())

        for key in key_list:
            skew = self.class_dict[key][part].skew()
            if not math.isnan(skew):
                skewness_arr.append(skew)

        return sum(skewness_arr)/len(skewness_arr)

    def show_skewness_histogram(self, part):
        '''
        The function returns the average skewness of all the classes
        Input:
        -part: string, the column for which to calculate skewness (pCount or iCount)
        Output:
        -histogram for overall skewness
        '''
        skewness_arr = []
        key_arr = []
        key_list = list(self.class_dict.keys())

        for key in key_list:
            skew = self.class_dict[key][part].skew()
            if not math.isnan(skew):
                skewness_arr.append(skew)
                key_arr.append(key)


        df = pd.DataFrame({'class':key_arr, 'skewness':skewness_arr})
        fig = px.histogram(df, x='skewness')
        fig.update_layout(height=500, width=800, title_x=0.5, title_text="Histogram of skewness values")
        return fig
  
    def get_average_kurtosis(self, part):
        '''
        The function returns the average kurtosis of all the classes
        Input:
        -part: string, the column for which to calculate kurtosis (pCount or iCount)
        Output:
        -kurtosis: float
        '''
        kurtosis_arr = []
        key_list = list(self.class_dict.keys())

        for key in key_list:
            skew = self.class_dict[key][part].kurtosis()
            if not math.isnan(skew):
                kurtosis_arr.append(skew)

        return sum(kurtosis_arr)/len(kurtosis_arr)

    def show_kurtosis_histogram(self, part):
        '''
        The function returns the average kurtosis of all the classes
        Input:
        -part: string, the column for which to calculate kurtosis (pCount or iCount)
        Output:
        -plotly histogram
        '''
        kurtosis_arr = []
        key_list = list(self.class_dict.keys())

        for key in key_list:
            skew = self.class_dict[key][part].kurtosis()
            if not math.isnan(skew):
                kurtosis_arr.append(skew)

        df = pd.DataFrame({'kurtosis':kurtosis_arr})
        fig = px.histogram(df, x='kurtosis')
        fig.update_layout(height=500, width=800, title_x=0.5, title_text="Histogram of kurtosis values")
        return fig

  
    def get_total_entities(self):
        '''returns total entities'''
        sum = 0
        key_list = list(self.class_dict.keys())

        for key in key_list:
            sum += len(self.class_dict[key])

        return sum
  
    def show_gini_histogram(self, part):
        '''
        The function returns a histogram showing the distribution of gini values
        Input:
        -part: string, the column for which to calculate gini (pCount or iCount)
        Output:
        -plotly histogram
        '''
        gini_arr = []
        key_arr = []
        key_list = list(self.class_dict.keys())

        for key in key_list:
            gini = self.__gini(self.class_dict[key][part])
            if not math.isnan(gini):
                gini_arr.append(gini)
                key_arr.append(key)

        df = pd.DataFrame({'key_arr':key_arr, 'gini':gini_arr})
        fig = px.histogram(df, x='gini')
        fig.update_layout(height=500, width=800, title_x=0.5, title_text="Histogram of gini values")
        fig.show()
  
    def get_average_gini(self, part):
        '''
        The function returns the average gini value of all the classes
        Input:
        -part: string, the column for which to calculate gini (pCount or iCount)
        Output:
        -gini: float
        '''
        gini_arr = []
        key_list = list(self.class_dict.keys())

        for key in key_list:
            gini = self.__gini(self.class_dict[key][part])
            if not math.isnan(gini):
                gini_arr.append(gini)


        return sum(gini_arr)/len(gini_arr)

  
    def get_average_entities(self):
        #Returns average amount of entities
        entity_amount_list = []
        key_list = list(self.class_dict.keys())

        for key in key_list:
            entity_amount_list.append(len(self.class_dict[key]))

        return sum(entity_amount_list)/len(entity_amount_list)
  
    def show_entity_count_histogram(self):
        #returns histogram for entity count per each class
        entity_amount_list = []
        key_list = list(self.class_dict.keys())

        for key in key_list:
            entity_amount_list.append(len(self.class_dict[key]))

        df = pd.DataFrame({'class':key_list, 'entity_count':entity_amount_list})
        fig = px.histogram(df, x='entity_count')
        fig.update_layout(height=500, width=800, title_x=0.5, title_text="Histogram of entity count")
        return fig

    def show_palma_histogram(self, part):
        '''
        The function returns a histogram showing the distribution of palma values
        Input:
        -part: string, the column for which to calculate palma value (pCount or iCount)
        Output:
        -plotly histogram for palma
        '''
        palma_arr = []
        key_arr = []
        key_list = list(self.class_dict.keys())

        for key in key_list:
            palma = self.__palma(self.class_dict[key][part])
            if not math.isnan(palma):
                palma_arr.append(palma)
                key_arr.append(key)

        df = pd.DataFrame({'class':key_arr, 'palma':palma_arr})
        fig = px.histogram(df, x='palma')
        fig.update_layout(height=500, width=800, title_x=0.5, title_text="Histogram of palma ratios")
        fig.show()

    def get_average_palma(self, part):
        '''
        The function returns the average palma value of all the classes
        Input:
        -part: string, the column for which to calculate palma value (pCount or iCount)
        Output:
        -palma value: float
        '''
        palma_arr = []
        key_arr = []
        key_list = list(self.class_dict.keys())

        for key in key_list:
            palma = self.__palma(self.class_dict[key][part])
            if not math.isnan(palma):
                palma_arr.append(palma)
                key_arr.append(key)

        df = pd.DataFrame({'class':key_arr, 'palma':palma_arr})
        return df['palma'].mean()

    def __gini(self, df):
        #Courtesy of Nurul Srianda
        arr = np.array(df)
        count = arr.size
        coefficient = 2 / count
        indexes = np.arange(1, count + 1)
        weighted_sum = (indexes * arr).sum()
        total = arr.sum()
        constant = (count + 1) / count
        return coefficient * weighted_sum / total - constant
  
    def __lorenz(self, df):
        #Courtesy of Nurul Srianda
        arr = np.array(df)
        # this divides the prefix sum by the total sum
        # this ensures all the values are between 0 and 1.0
        scaled_prefix_sum = arr.cumsum() / arr.sum()
        # this prepends the 0 value (because 0% of all people have 0% of all wealth)
        return np.insert(scaled_prefix_sum, 0, 0)
  
    def __palma(self, df):
        #Courtesy of Nurul Srianda
        lorenz_arr = self.__lorenz(df)
        return (1 - np.quantile(lorenz_arr, 0.9)) / np.quantile(lorenz_arr, 0.4)


    def show_pareto_all(self, title_text, part):
        '''
        This function creates a subplot filled with each class's pareto chart
        Input:
        -title_text: string, for setting the title of the plot
        -part: part of the dataframe to be mapped to histogram ("pCount", "iCount")
        
        output:
        -plotly figure for pareto chart
        '''
        key_list = list(self.class_dict.keys())
        key_list.sort()

        rows = math.ceil(len(key_list)/5)
        height = 2*120*rows

        subplot_titles = []
        for i in range(len(key_list)):
            subplot_titles.append("title")

        fig = make_subplots(
        rows=rows, cols=5, subplot_titles=subplot_titles
        )
        row = 1
        col = 1
        index = 0
        for key in tqdm(key_list):
            df = self.class_dict[key].copy()
            #fig.add_trace(go.Histogram(x=self.class_dict[key][part]), row=row, col=col)
            df_sorted = df.sort_values(by=part, ascending=False)
            df_sorted.reset_index(inplace=True)
            df_sorted['cumperc'] = df_sorted[part].cumsum()/df_sorted[part].sum()*100

            trace1 = go.Bar(
            x=df_sorted.index,
            y=df_sorted[part],
            name='Property Count',
            marker=dict(
                color='rgb(34,163,192)'
                      ),
            hoverinfo='skip',
            )
            trace2 = go.Scatter(
              x=df_sorted.index,
              y=df_sorted['cumperc'],
              name='Cumulative Percentage',
              yaxis='y2',
              hoverinfo='skip'

            )

            fig.add_trace(trace1, row=row, col=col)
            fig.add_trace(trace2, row=row, col=col)
            fig.layout.annotations[index].update(text=key)


            index += 1
            col += 1
            if col > 5:
                col = 1
                row += 1

        fig.update_layout(height=height, width=1200, title_text=title_text, title_x=0.5, showlegend=False)
        return fig
    
    def get_emd_distance_matrix(self, part):
        '''
        This function returns a distance matrix between each class with
        "earth mover's distance" algorithm being used to calculate distance.
        The order of each row is based on the class key list sorted. The distance matrix then can be used for clustering purposes
        Input:
        -part: part of the dataframe to be calculated ("pCount", "iCount")
        
        output:
        -distance matrix: numpy array
        '''
        class_dict = self.class_dict
        key_list = list(class_dict.keys())
        key_list.sort()

        distance_matrix = []

        for key in tqdm(key_list):
            row = []
            for other_key in key_list:
                if other_key == key:
                    row.append(0)
                else:
                    u_values = list(class_dict[key][part])
                    v_values = list(class_dict[other_key][part])
                    emd = wasserstein_distance(u_values, v_values)
                    row.append(emd)
            distance_matrix.append(row)

        return np.array(distance_matrix)
    
    def get_ks_distance_matrix(self, part):
        '''
        This function returns a distance matrix between each class with 
        "Kolmogorov–Smirnov test" algorithm being used to calculate distance between each class.
        The order of each row is based on the class key list sorted. The distance matrix then can be used for clustering purposes.
        Input:
        -part: part of the dataframe to be calculated ("pCount", "iCount")
        
        output:
        -distance matrix: numpy array
        '''
        class_dict = self.class_dict
        key_list = list(class_dict.keys())
        key_list.sort()

        distance_matrix = []

        for key in tqdm(key_list):
            row = []
            for other_key in key_list:
                if other_key == key:
                    row.append(0)
                else:
                    u_cdf = norm.cdf(list(class_dict[key][part]))
                    v_cdf = norm.cdf(list(class_dict[other_key][part]))
                    ks, pval = ks_2samp(u_cdf, v_cdf)
                    row.append(ks)
            distance_matrix.append(row)

        return np.array(distance_matrix)