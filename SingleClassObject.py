#Import the libraries needed
import pandas as pd
import numpy as np
import plotly.express as px
import matplotlib.pyplot as plt
import math
import plotly.graph_objects as go
import time
from plotly.subplots import make_subplots

class WealthKGSingleClassObject:
    '''
    Wealth KG Object for entities within a single class

    Attributes:
    - Dataframe: Pandas Dataframe. Dataframe of entities with incoming properties, outgoing properties, total properties.
    - bag: Boolean. True if bag was used for queries, false if set was used for queries.
    - class_filter: string. Represents the class of this analysis.
    - entity_count: int. Represents number of entities in analysis.
    '''
    def __init__(self, dataframe, bag, class_filter, entity_count):
        self.dataframe = dataframe
        self.bag = bag
        self.class_filter = class_filter
        self.entity_count = entity_count
  
    def get_summary(self, part):
        #Returns a summary for the class based on a part
        df = self.dataframe
        print("Q1 =", df[part].quantile(.25))
        print("Q2/median =", df[part].median())
        print("Q3 =", df[part].quantile(.75))
        print("mode =", df[part].mode()[0])
        print("mean =", df[part].mean())
        print("kurtosis =", df[part].kurtosis())
        print("skewness =", df[part].skew())
        print("..........")
        print("")
  
    def gini(self, part):
        #courtesy of Nurul Srianda
        #Calculates gini value
        arr = list(self.dataframe[part])
        count = arr.size
        coefficient = 2 / count
        indexes = np.arange(1, count + 1)
        weighted_sum = (indexes * arr).sum()
        total = arr.sum()
        constant = (count + 1) / count
        return coefficient * weighted_sum / total - constant
  
    def lorenz(self, part):
        #Courtesy of Nurul Srianda
        #Calculates lorenz value
        arr = list(self.dataframe[part])
        # this divides the prefix sum by the total sum
        # this ensures all the values are between 0 and 1.0
        scaled_prefix_sum = arr.cumsum() / arr.sum()
        # this prepends the 0 value (because 0% of all people have 0% of all wealth)
        return np.insert(scaled_prefix_sum, 0, 0)
  
    def palma(self, part):
        #Courtesy of Nurul Srianda
        #Calculates palma value
        lorenz_arr = self.lorenz(part)
        return (1 - np.quantile(lorenz_arr, 0.9)) / np.quantile(lorenz_arr, 0.4)
    
    def get_histogram(self, part):
        #Returns a plotly histogram for a part
        #Input: part: string, which column to visualize
        #Output: plotly histogram
        height = 500
        width = 800
        if part == "iCount":
            fig = px.histogram(self.dataframe, x='iCount', title = "Incoming Properties")
        elif part == "pCount":
            fig = px.histogram(self.dataframe, x='pCount', title = "Outgoing Properties")
        elif part =="totalCount":
            fig = px.histogram(self.dataframe, x='totalCount', title="Total Properties" )
        else:
            bin_size = 5
            if self.dataframe['totalCount'].max() > 100:
                bin_size = 10
            if self.dataframe['totalCount'].max() < 20:
                bin_size = 19
            fig = make_subplots(rows=3, cols=1,  subplot_titles=['Incoming Properties', 'Outgoing Properties', 'All Properties'])
            fig.add_trace(go.Histogram(x=self.dataframe.iCount, xbins={'size':bin_size}), row=1, col=1)
            fig.add_trace(go.Histogram(x=self.dataframe.pCount, xbins={'size':bin_size}), row=2, col=1)
            fig.add_trace(go.Histogram(x=self.dataframe.totalCount, xbins={'size':bin_size}), row=3, col=1)
            height = 900
            width = 800

        fig.update_layout(height=height, width=width, title_text="Class Filters: {}".format(self.class_filter), showlegend=False)

        return fig
  
    def get_pareto_chart(self, part):
        #Courtesy of Nurul Srianda
        ### This function plots the Pareto chart of a given a class based on a part
        ### INPUT: part: string, which column to visualize
        ### OUTPUT: Pareto chart
        ### source = https://stackoverflow.com/questions/62287001/how-to-overlay-two-plots-in-same-figure-in-plotly-create-pareto-chart-in-plotl
        df = self.dataframe
        df_sorted = df.sort_values(by=part, ascending=False)
        df_sorted.reset_index(inplace=True)
        df_sorted['cumperc'] = df_sorted[part].cumsum()/df_sorted[part].sum()*100

        trace1 = go.Bar(
          x=df_sorted.index,
          y=df_sorted[part],
          name='Property Count',
          marker=dict(
              color='rgb(34,163,192)'
                    )
        )
        trace2 = go.Scatter(
            x=df_sorted.index,
            y=df_sorted['cumperc'],
            name='Cumulative Percentage',
            yaxis='y2'

        )

        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(trace1)
        fig.add_trace(trace2,secondary_y=True)
        fig['layout'].update(height = 700, width = 1300, title = "Pareto Chart",xaxis=dict(
              tickangle=-90
            ))

        return fig