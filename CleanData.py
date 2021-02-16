# Theo Chambers - BigData# 

import pandas as pd 
import numpy as np

# make sure working directory is set to source file location
# Files needed : combined_data_1.txt,movie_titles.csv
# This program outputs the cleaned data from the netflix movie_data file 

# main function # 
def main():
    # open combine_data.txt #
    collection1 = open('combined_data_1.txt','r')
    line = collection1.readlines()
    collection1.close()
    #if line ends with :, replace with space
    data1 = []
    for i in range(len(line)):
        line[i] = line[i].strip('\n')
        if line[i].endswith(':'):
            # movie_id is where line contains colon
            movie_id = line[i].replace(':','')
        else:
            # create new_rows with values from split
            new_rows = line[i].split(',')
            # insert at position 0, movie_id
            new_rows.insert(0,movie_id)
            #append new_rows to data1 list
            data1.append(new_rows)
    #write to csv from data1 
    data = open('data_collection_1.csv', mode='w')
    for i in data1:
        data.write(','.join(i))
        data.write('\n')
    # close file to not exceed memory limit
    data.close()
    #read in df1 from data_collection_1.csv
    df1 = pd.read_csv('data_collection_1.csv',usecols=[0,1,2,3],names=['movie_id','customer_id','rating','timestamp'])
    movie_titles=pd.read_csv('movietitles.csv',encoding='ISO-8859-1',sep=',',usecols=[0,1,2],names=['movie_id','release_date','movie_name'])
    movie_titles = movie_titles.drop(columns='release_date')
    # merge df1 - inner join on movie_id to keep values
    final_df = df1.merge(movie_titles,how='inner',on='movie_id')
    # groupby movies and rating, agg command to find count and mean rating
    df_movies = final_df.groupby('movie_id')['rating'].agg(['count','mean'])
    #map index to int, used to drop quantiles later
    df_movies.index = df_movies.index.map(int)

    #initialize quantiles
    #low_quantile and high_quantile produce number where ratings count per movie_id fits the quantile
    low_quantile = round(df_movies['count'].quantile(.10),0)
    high_quantile = round(df_movies['count'].quantile(.70),0)

    # final _df is everything without drop_outliers_low and drop_outliers_high
    # we get indices returned of all values that can be considered in low and high quantiles
    drop_outliers_low = df_movies[df_movies['count'] < low_quantile].index
    drop_outliers_high = df_movies[df_movies['count']>high_quantile].index
    #isin command produces booleans where it will return true if the dataframe has values on condition
    final_df = final_df[~final_df['movie_id'].isin(drop_outliers_low)]
    final_df = final_df[~final_df['movie_id'].isin(drop_outliers_high)]
    #use numpy to save txt as CleanMovieData.txt
    np.savetxt('CleanMovieData.txt',final_df.values,fmt='%s',delimiter='\t')

if __name__=='__main__':
    main()
